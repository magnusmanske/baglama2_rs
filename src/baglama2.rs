use crate::category::{category_tree, CategoryTitle};
use crate::config::Config;
use crate::db::{sql_placeholders, Db};
use crate::group_id::GroupId;
use crate::row_group::{GroupSource, RowGroup};
use crate::wiki::{Dbname, DumpCode};
use crate::Site;
use anyhow::{anyhow, Result};
use log::{error, info, warn};
use mysql_async::{from_row, from_row_opt, prelude::*};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use wikimisc::mediawiki::action_api::{ActionApi, ActionApiRunnable};
use wikimisc::mediawiki::Api;
use wikimisc::site_matrix::SiteMatrix;

/// Language wikis in a `sitematrix` API response: dbname → (language code,
/// English language name). Special wikis (Commons, Meta, chapters, test
/// wikis) are not listed under a language, so they are not included.
fn wiki_languages(matrix: &Value) -> HashMap<String, (String, String)> {
    let mut ret = HashMap::new();
    let Some(entries) = matrix["sitematrix"].as_object() else {
        return ret;
    };
    for (key, language) in entries {
        if key == "count" || key == "specials" {
            continue;
        }
        let (Some(code), Some(name)) = (language["code"].as_str(), language["localname"].as_str())
        else {
            continue;
        };
        for site in language["site"].as_array().into_iter().flatten() {
            if let Some(dbname) = site["dbname"].as_str() {
                ret.insert(dbname.to_string(), (code.to_string(), name.to_string()));
            }
        }
    }
    ret
}

/// `(giu_code, name)` for each language wiki in `sites` that has no name yet.
///
/// The name is the one already used for another wiki in the same language,
/// so the labels stay consistent (the hand-set "Azeri" over the site
/// matrix's "Azerbaijani"); if several are in use, the most common, then the
/// alphabetically first. Failing that, the site matrix's English name.
/// Wikis that are not language wikis keep no name: `ar.wikimedia.org` is
/// Wikimedia Argentina, not Arabic.
fn site_names_to_fill(
    sites: &[(String, Option<String>)],
    languages: &HashMap<String, (String, String)>,
) -> Vec<(String, String)> {
    let mut in_use: HashMap<&str, HashMap<&str, usize>> = HashMap::new();
    for (giu, name) in sites {
        if let (Some(name), Some((code, _))) = (name, languages.get(giu)) {
            *in_use.entry(code).or_default().entry(name).or_default() += 1;
        }
    }
    let preferred: HashMap<&str, &str> = in_use
        .into_iter()
        .filter_map(|(code, names)| {
            let best = names
                .into_iter()
                .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(a.0)))?;
            Some((code, best.0))
        })
        .collect();
    let mut ret = vec![];
    for (giu, name) in sites {
        let Some((code, matrix_name)) = languages.get(giu) else {
            continue;
        };
        if name.is_some() {
            continue;
        }
        let label = preferred.get(code.as_str()).copied().unwrap_or(matrix_name);
        if !label.is_empty() {
            ret.push((giu.clone(), label.to_string()));
        }
    }
    ret
}

/// Titles per `IN (…)` list. MySQL allows 65,535 placeholders per statement,
/// and a tree level at depth 5 can have more categories than that (group 903
/// failed on one in 2026-01).
pub const IN_CHUNK: usize = 1000;

/// Rows in flight between a file query and the usage queries that consume
/// them; a few of the 3,000-file batches those run on.
const FILE_STREAM_BUFFER: usize = 10_000;

/// Tables a category-membership query reads.
const CATEGORY_TABLES: [&str; 3] = ["page", "categorylinks", "linktarget"];

/// Most groups [`Baglama2::deactivate_nonexistent_categories`] switches off
/// in one run. Categories rarely disappear; many missing at once means the
/// Commons lookup went wrong (a replica problem, another table split).
const MAX_DEACTIVATIONS: usize = 20;

/// The `(id, category)` groups whose category is not in `existing`, or an
/// error if there are more than `max`.
fn groups_to_deactivate(
    groups: &[(GroupId, CategoryTitle)],
    existing: &HashSet<CategoryTitle>,
    max: usize,
) -> Result<Vec<(GroupId, CategoryTitle)>> {
    let gone: Vec<(GroupId, CategoryTitle)> = groups
        .iter()
        .filter(|(_, category)| !existing.contains(category))
        .cloned()
        .collect();
    if gone.len() > max {
        return Err(anyhow!(
            "{} of {} categories not found on Commons, more than the limit of {max}; \
             check the lookup before trusting that",
            gone.len(),
            groups.len()
        ));
    }
    Ok(gone)
}

#[derive(Debug)]
pub struct Baglama2 {
    config: Config,
    db: Arc<Db>,
    sites_cache: Vec<Site>,
    site_matrix: SiteMatrix,
}

impl Baglama2 {
    /// Opens the DB pools, fetches the site matrix and loads the sites table.
    pub async fn new(config: Config) -> Result<Self> {
        let db = Arc::new(Db::new(&config)?);
        info!("Baglama2::new: connecting to Wikidata API");
        let wikidata_api = Api::new("https://www.wikidata.org/w/api.php").await?;
        info!("Baglama2::new: building site matrix from Wikidata");
        let mut ret = Self {
            config,
            db,
            sites_cache: vec![],
            site_matrix: SiteMatrix::new(&wikidata_api).await?,
        };
        info!("Baglama2::new: populating sites cache from tool DB");
        ret.populate_sites().await?;
        info!("Baglama2::new: ready");
        Ok(ret)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// The code a wiki has in the pageview dumps: its host name without
    /// `.org`, e.g. `enwiki` → `en.wikipedia`, `wikidatawiki` →
    /// `wikidata`. The site matrix comes first, because `sites.server` in
    /// the tool DB is wrong for several special wikis (`meta.wikipedia.org`);
    /// but the site matrix omits closed wikis, which still get views.
    pub fn wiki_dump_code(&self, wiki: &Dbname) -> Option<DumpCode> {
        if let Ok(url) = self.site_matrix.get_server_url_for_wiki(wiki.as_str()) {
            return DumpCode::from_server(&url);
        }
        let site = self
            .sites_cache
            .iter()
            .find(|s| s.giu_code() == Some(wiki))?;
        DumpCode::from_server(site.server().as_deref()?)
    }

    /// Switches off groups whose category no longer exists on Commons.
    ///
    /// Nothing switches groups back on, so this refuses to act on more than
    /// [`MAX_DEACTIVATIONS`] at once; see [`groups_to_deactivate`].
    pub async fn deactivate_nonexistent_categories(&self) -> Result<()> {
        let sql = format!(
            "{} WHERE is_user_name=0 AND is_active=1",
            RowGroup::sql_select()
        );
        info!("deactivate_nonexistent_categories: querying active groups from tool DB");
        let rows = self
            .db
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row_opt::<RowGroup>)
            .await?;
        let mut groups: Vec<(GroupId, CategoryTitle)> = vec![];
        for row in rows {
            match row {
                Ok(group) => {
                    if let GroupSource::Category { title, .. } = group.source() {
                        groups.push((group.id(), title.clone()));
                    }
                }
                // Left alone: it cannot be checked, so it is not known to be gone.
                Err(e) => warn!("deactivate_nonexistent_categories: skipping {e}"),
            }
        }
        let categories: Vec<CategoryTitle> = groups.iter().map(|(_, cat)| cat.clone()).collect();
        let existing = self.get_existing_categories(&categories).await?;
        info!(
            "deactivate_nonexistent_categories: {} of {} categories exist on Commons",
            existing.len(),
            groups.len()
        );
        let gone = match groups_to_deactivate(&groups, &existing, MAX_DEACTIVATIONS) {
            Ok(gone) => gone,
            Err(e) => {
                // Not fatal: the month runs with the groups as they are.
                error!("deactivate_nonexistent_categories: {e:#}; deactivating none");
                return Ok(());
            }
        };
        for (id, category) in &gone {
            warn!("deactivating group {id}: Category:{category} does not exist on Commons");
        }
        if gone.is_empty() {
            return Ok(());
        }
        let ids: Vec<GroupId> = gone.iter().map(|(id, _)| *id).collect();
        self.deactivate_groups(&ids).await
    }

    async fn deactivate_groups(&self, group_ids: &[GroupId]) -> Result<()> {
        let placeholders = sql_placeholders(group_ids.len());
        let sql = format!("UPDATE `groups` SET is_active=0 WHERE id IN ({placeholders})");
        self.db
            .get_tooldb_conn()
            .await?
            .exec_drop(sql, group_ids.iter().map(|id| id.get()).collect::<Vec<_>>())
            .await?;
        Ok(())
    }

    /// Those of `categories` that exist on Commons.
    async fn get_existing_categories(
        &self,
        categories: &[CategoryTitle],
    ) -> Result<HashSet<CategoryTitle>> {
        if categories.is_empty() {
            return Ok(HashSet::new());
        }
        let keys: Vec<String> = categories.iter().map(CategoryTitle::db_key).collect();
        let sql = format!(
            "SELECT FROM_BASE64(TO_BASE64(page_title)) FROM `page` WHERE `page_namespace`=14 AND `page_title` IN ({})",
            sql_placeholders(keys.len())
        );
        info!(
            "get_existing_categories: running single IN query against Commons `page` with {} placeholders",
            keys.len()
        );
        let found: Vec<String> = self.db.query_commons(&["page"], &sql, keys).await?;
        info!(
            "get_existing_categories: Commons query returned {} rows",
            found.len()
        );
        Ok(found
            .iter()
            .filter_map(|title| CategoryTitle::parse(title).ok())
            .collect())
    }

    async fn populate_sites(&mut self) -> Result<()> {
        let sql = "SELECT server,giu_code FROM `sites`";
        self.sites_cache = self
            .db
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<Site>)
            .await?;
        Ok(())
    }

    // TESTED
    pub fn get_sites(&self) -> Result<Vec<Site>> {
        Ok(self.sites_cache.clone())
    }

    /// Updates sites in the tooldb from the Commons database
    pub async fn update_sites(&self) -> Result<()> {
        info!("update_sites: fetching site list from Commons DB");
        let sites = self.get_sites_from_commons_db().await?;
        info!(
            "update_sites: got {} sites; upserting into tool DB",
            sites.len()
        );
        self.ensure_sites_in_tooldb(sites).await?;
        info!("update_sites: tool DB sites table updated");
        // Labels are cosmetic; a run must not fail over them.
        if let Err(e) = self.update_site_names().await {
            warn!("update_sites: could not fill in language names: {e:#}");
        }
        Ok(())
    }

    /// Fills in `sites.name`, the language name the web interface shows
    /// ("Arabic Wiktionary" rather than "ar.Wiktionary"), where it is missing.
    /// Existing names are never changed. See [`site_names_to_fill`].
    async fn update_site_names(&self) -> Result<()> {
        let api = Api::new("https://meta.wikimedia.org/w/api.php").await?;
        let matrix = ActionApi::sitematrix().run(&api).await?;
        let languages = wiki_languages(&matrix);
        if languages.is_empty() {
            return Err(anyhow!("site matrix lists no language wikis"));
        }
        let mut conn = self.db.get_tooldb_conn().await?;
        let sites: Vec<(String, Option<String>)> =
            conn.query("SELECT giu_code,name FROM `sites`").await?;
        let fill = site_names_to_fill(&sites, &languages);
        if fill.is_empty() {
            return Ok(());
        }
        conn.exec_batch(
            "UPDATE `sites` SET name=? WHERE giu_code=? AND name IS NULL",
            fill.iter().map(|(giu, name)| (name, giu)),
        )
        .await?;
        info!("update_sites: filled in {} language names", fill.len());
        Ok(())
    }

    /// Adds the wikis that the `sites` table does not have yet.
    ///
    /// Only new ones are sent: InnoDB reserves an auto-increment ID for every
    /// row of a multi-row `INSERT IGNORE`, including the ignored ones, so
    /// sending all ~1,100 wikis each run used up that many IDs.
    async fn ensure_sites_in_tooldb(
        &self,
        sites: Vec<(String, String, String, String)>,
    ) -> Result<()> {
        let mut conn = self.db.get_tooldb_conn().await?;
        let known: HashSet<String> = conn
            .query::<String, _>("SELECT giu_code FROM `sites`")
            .await?
            .into_iter()
            .collect();
        let new: Vec<_> = sites
            .into_iter()
            .filter(|(_, giu_code, _, _)| !known.contains(giu_code))
            .collect();
        if new.is_empty() {
            return Ok(());
        }
        info!("update_sites: adding {} new wikis", new.len());
        // IGNORE still covers a new wiki that clashes on `server` or
        // `(project, language)`.
        let placeholders = vec!["(?,?,?,?)"; new.len()].join(",");
        let sql = format!(
            "INSERT IGNORE INTO `sites` (server,giu_code,project,language) VALUES {placeholders}"
        );
        let params = new
            .iter()
            .flat_map(|(server, giu_code, project, language)| [server, giu_code, project, language])
            .collect::<Vec<_>>();
        conn.exec_drop(sql, params).await?;
        Ok(())
    }

    async fn get_sites_from_commons_db(&self) -> Result<Vec<(String, String, String, String)>> {
        let sql = r"SELECT
        substr(reverse(site_domain),2) as `server`,
        site_global_key as `giu_code`,
        site_group as `project`,
        regexp_replace(substr(reverse(site_domain),2),'\\..*$','') as `language`
        FROM sites";
        let sites = self
            .db
            .get_commons_conn_for_tables(&["sites"])
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(String, String, String, String)>)
            .await?;
        Ok(sites)
    }

    // TESTED
    /// The DB keys of a group's category tree (see [`category_tree`]).
    pub async fn category_tree_of(
        &self,
        category: &CategoryTitle,
        depth: isize,
    ) -> Result<Vec<String>> {
        category_tree(category, depth, |cats| async move {
            let mut children = vec![];
            for chunk in cats.chunks(IN_CHUNK) {
                // No DISTINCT: `category_tree` drops repeats, and a sort on
                // the server would hold up the rows.
                let sql = format!(
                    "SELECT FROM_BASE64(TO_BASE64(page_title))
                    FROM page,categorylinks,linktarget
                    WHERE page_id=cl_from
                    AND cl_target_id=lt_id AND lt_namespace=14
                    AND lt_title IN ({})
                    AND cl_type='subcat'",
                    sql_placeholders(chunk.len())
                );
                let mut found: Vec<String> = self
                    .db
                    .query_commons(&CATEGORY_TABLES, &sql, chunk.to_vec())
                    .await?;
                children.append(&mut found);
            }
            Ok(children)
        })
        .await
    }

    /// The files (DB keys, no redirects) directly in `categories`, at most
    /// [`IN_CHUNK`] of them, streamed as they arrive. A file in several
    /// categories comes more than once; the receiver de-duplicates. The
    /// server sorts nothing, so even a category with millions of files
    /// (4.8M in "Uploaded with OpenRefine") streams at once.
    pub fn stream_files_in_categories(
        &self,
        categories: &[String],
    ) -> Result<mpsc::Receiver<Result<String>>> {
        if categories.len() > IN_CHUNK {
            return Err(anyhow!(
                "stream_files_in_categories: {} categories, at most {IN_CHUNK} per query",
                categories.len()
            ));
        }
        let sql = format!(
            "SELECT FROM_BASE64(TO_BASE64(page_title))
            FROM page,categorylinks,linktarget
            WHERE cl_from=page_id AND page_namespace=6
            AND cl_target_id=lt_id AND lt_namespace=14
            AND lt_title IN ({})
            AND page_is_redirect=0",
            sql_placeholders(categories.len())
        );
        self.db.stream_commons(
            &CATEGORY_TABLES,
            &sql,
            categories.to_vec(),
            FILE_STREAM_BUFFER,
        )
    }

    /// Gets all images uploaded by a user
    pub async fn get_files_from_user_name(&self, user_name: &str) -> Result<Vec<String>> {
        let sql = "SELECT DISTINCT FROM_BASE64(TO_BASE64(img_name)) FROM image,actor,user WHERE img_actor=actor_id AND user_name=:user_name AND user_id=actor_user";
        self.db
            .query_commons(
                &["image", "actor", "user"],
                sql,
                mysql_async::params! {user_name},
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that the HashSet-based deduplication used inside find_subcats
    /// correctly collapses duplicate category names that appear across multiple
    /// discovery rounds, without requiring a DB connection.
    #[test]
    fn test_find_subcats_dedup_logic() {
        // Simulate two discovery rounds where "Cat:A" appears in both.
        let round1 = ["Cat:A".to_string(), "Cat:B".to_string()];
        let round2 = ["Cat:A".to_string(), "Cat:C".to_string()];

        let mut seen: HashSet<String> = HashSet::new();
        for item in round1.iter().chain(round2.iter()) {
            seen.insert(item.clone());
        }

        let mut result: Vec<String> = seen.into_iter().collect();
        result.sort();

        assert_eq!(
            result,
            vec![
                "Cat:A".to_string(),
                "Cat:B".to_string(),
                "Cat:C".to_string()
            ]
        );
    }

    /// find_subcats must not revisit a category that is already in `subcats`,
    /// even when the incoming `check` list contains it again.
    #[test]
    fn test_find_subcats_already_seen_categories_are_filtered() {
        let mut seen: HashSet<String> = HashSet::new();
        seen.insert("Cat:A".to_string());

        // Simulate `remaining` computation for the next round.
        let check = vec!["Cat:A".to_string(), "Cat:B".to_string()];
        let remaining: Vec<String> = check.into_iter().filter(|c| !seen.contains(c)).collect();

        assert_eq!(remaining, vec!["Cat:B".to_string()]);
    }

    #[test]
    fn test_groups_to_deactivate() {
        let cat = |s: &str| CategoryTitle::parse(s).unwrap();
        let groups: Vec<(GroupId, CategoryTitle)> = (1..=30)
            .map(|i| (GroupId::new(i).unwrap(), cat(&format!("Cat {i}"))))
            .collect();
        let all: HashSet<CategoryTitle> = groups.iter().map(|(_, c)| c.clone()).collect();
        assert!(groups_to_deactivate(&groups, &all, 20).unwrap().is_empty());
        let mut some = all.clone();
        some.remove(&cat("Cat 7"));
        assert_eq!(
            groups_to_deactivate(&groups, &some, 20).unwrap(),
            vec![(GroupId::new(7).unwrap(), cat("Cat 7"))]
        );
        // Commons spells it with underscores: still the same category.
        let from_commons: HashSet<CategoryTitle> =
            (1..=30).map(|i| cat(&format!("Cat_{i}"))).collect();
        assert!(groups_to_deactivate(&groups, &from_commons, 20)
            .unwrap()
            .is_empty());
        // An empty lookup result would switch off everything: refused.
        assert!(groups_to_deactivate(&groups, &HashSet::new(), 20).is_err());
        assert_eq!(
            groups_to_deactivate(&groups, &HashSet::new(), 30)
                .unwrap()
                .len(),
            30
        );
    }

    fn test_matrix() -> Value {
        serde_json::json!({"sitematrix": {
            "count": 5,
            "0": {"code": "ar", "localname": "Arabic", "site": [
                {"dbname": "arwiki"}, {"dbname": "arwiktionary"}, {"dbname": "arwikibooks"}]},
            "1": {"code": "az", "localname": "Azerbaijani", "site": [
                {"dbname": "azwiki"}, {"dbname": "azwikiquote"}]},
            "2": {"code": "xx", "localname": "Ex", "site": [
                {"dbname": "xxwiki"}, {"dbname": "xxwiktionary"}, {"dbname": "xxwikibooks"}]},
            "specials": [{"dbname": "arwikimedia"}, {"dbname": "commonswiki"}]
        }})
    }

    #[test]
    fn test_wiki_languages() {
        let languages = wiki_languages(&test_matrix());
        assert_eq!(
            languages.get("arwiktionary"),
            Some(&("ar".to_string(), "Arabic".to_string()))
        );
        assert!(!languages.contains_key("arwikimedia"));
        assert!(!languages.contains_key("commonswiki"));
        assert!(wiki_languages(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn test_site_names_to_fill() {
        let site = |giu: &str, name: Option<&str>| (giu.to_string(), name.map(String::from));
        let sites = vec![
            site("arwiki", Some("Arabic")),
            site("arwiktionary", None),
            site("arwikimedia", None), // Wikimedia Argentina, not Arabic
            site("azwiki", Some("Azeri")),
            site("azwikiquote", None),
            site("xxwiki", Some("B")),
            site("xxwiktionary", Some("A")),
            site("xxwikibooks", None),
            site("commonswiki", Some("Commons")),
            site("unknownwiki", None),
        ];
        let mut fill = site_names_to_fill(&sites, &wiki_languages(&test_matrix()));
        fill.sort();
        let expected = vec![
            ("arwiktionary".to_string(), "Arabic".to_string()),
            // An existing label beats the site matrix name.
            ("azwikiquote".to_string(), "Azeri".to_string()),
            // A tie goes to the alphabetically first.
            ("xxwikibooks".to_string(), "A".to_string()),
        ];
        assert_eq!(fill, expected);
        // Without any existing label, the site matrix name is used.
        let fill = site_names_to_fill(
            &[site("arwikibooks", None)],
            &wiki_languages(&test_matrix()),
        );
        assert_eq!(
            fill,
            vec![("arwikibooks".to_string(), "Arabic".to_string())]
        );
    }

    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_get_sites() {
        let baglama = Baglama2::new(Config::load().unwrap()).await.unwrap();
        let sites1 = baglama.get_sites().unwrap(); // Raw
        let sites2 = baglama.get_sites().unwrap(); // From cache
        assert_eq!(sites1.len(), sites2.len());
        assert!(sites1
            .iter()
            .any(|site| *site.server() == Some("zh-min-nan.wiktionary.org".to_string())));
    }

    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_get_pages_in_category() {
        let baglama = Baglama2::new(Config::load().unwrap()).await.unwrap();
        let blue_sky = CategoryTitle::parse("Blue sky in Berlin").unwrap();
        // Depth 0 is the category alone, and each level adds subcategories.
        let mut last = 0;
        let mut cats = vec![];
        for depth in 0..3 {
            cats = baglama.category_tree_of(&blue_sky, depth).await.unwrap();
            assert!(cats.contains(&"Blue_sky_in_Berlin".to_string()));
            assert!(cats.len() > last, "depth {depth}: {cats:?}");
            last = cats.len();
        }
        let mut rx = baglama.stream_files_in_categories(&cats).unwrap();
        let mut files = vec![];
        while let Some(file) = rx.recv().await {
            files.push(file.unwrap());
        }
        assert!(files.contains(&"2013-06-07_Kindergartenfest_Berlin-Karow_03.jpg".to_string()));
        assert!(baglama
            .stream_files_in_categories(&vec![String::new(); IN_CHUNK + 1])
            .is_err());
    }

    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_get_files_from_user_name() {
        let baglama = Baglama2::new(Config::load().unwrap()).await.unwrap();
        let files = baglama
            .get_files_from_user_name("Magnus Manske")
            .await
            .unwrap();
        assert!(files.contains(&"2002-07_Sylt_-_Westerland_(panorama).jpg".to_string()));
    }
}
