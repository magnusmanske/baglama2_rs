use crate::row_group::RowGroup;
use crate::DbId;
use crate::GroupId;
use crate::Site;
use anyhow::{anyhow, Result};
use core::time::Duration;
use log::{info, warn};
use mysql_async::{from_row, prelude::*, Conn};

use serde_json::Value;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::path::Path;

use wikimisc::mediawiki::Api;
use wikimisc::site_matrix::SiteMatrix;
use wikimisc::toolforge_db::{DatabaseError, DbCluster, ToolforgeDB};

/// The wiki whose replica databases this tool reads. Baglama only queries
/// Commons; the links split below applies to no other wiki.
const COMMONS_WIKI: &str = "commonswiki";

/// Pool key of the Commons core replica (`commonswiki…`), holding every table
/// that was not split off.
const POOL_COMMONS_CORE: &str = "commons";

/// Pool key of the Commons links extension cluster (`links.commonswiki…`, the
/// `x4` section), which took over the links tables in September 2026.
///
/// A future split needs a new pool under its own key, registered in
/// [`Baglama2::new`] from config, plus a [`DbCluster`] arm in
/// [`Baglama2::commons_pool_key`].
const POOL_COMMONS_LINKS: &str = "commons_links";

/// MySQL/MariaDB error: the user has used up `max_user_connections`.
const ER_USER_LIMIT_REACHED: u16 = 1226;

/// MariaDB error: the query ran past `max_statement_time`.
const ER_STATEMENT_TIMEOUT: u16 = 1969;

/// Whether `e` is an error the server reported with `code`.
fn is_server_error(e: &mysql_async::Error, code: u16) -> bool {
    matches!(e, mysql_async::Error::Server(se) if se.code == code)
}

#[derive(Debug)]
pub struct Baglama2 {
    config: Value,
    tfdb: ToolforgeDB,
    sites_cache: Vec<Site>,
    site_matrix: SiteMatrix,
}

impl Baglama2 {
    /// Max time for a SINGLE connection-acquisition attempt. Generous because
    /// a cold pool / slow replica handshake on Toolforge can legitimately take
    /// tens of seconds; normal is well under a second.
    const DB_CONN_TIMEOUT: Duration = Duration::from_secs(60);

    /// How many times to retry connection acquisition before giving up.
    const DB_CONN_RETRIES: u32 = 4;

    /// How long to keep waiting for a connection while the server refuses it
    /// with `max_user_connections` (error 1226). The limit is per tool user
    /// across all processes, so it frees up as other queries end; waiting is
    /// better than failing a group.
    const DB_CONN_LIMIT_WAIT: Duration = Duration::from_secs(60 * 60);

    /// Server-side time limit for each attempt of a Commons query, in seconds;
    /// one entry per attempt. Grows because a query that just missed the limit
    /// on a loaded replica usually succeeds with a bit more time.
    const DB_QUERY_TIME_LIMITS: [u64; 5] = [600, 900, 1200, 1500, 1800];

    /// Extra time the client waits beyond the server-side limit before giving
    /// up itself. Only a backstop: normally the server ends the query first.
    const DB_QUERY_CLIENT_GRACE: Duration = Duration::from_secs(60);

    pub async fn new() -> Result<Self> {
        let config = match Self::get_config_from_file("config.json") {
            Ok(config) => config,
            Err(_) => {
                Self::get_config_from_file("/data/project/glamtools/baglama2_rs/config.json")?
            }
        };
        info!("Baglama2::new: connecting to Wikidata API");
        let wikidata_api = Api::new("https://www.wikidata.org/w/api.php").await?;
        info!("Baglama2::new: building site matrix from Wikidata");
        let mut ret = Self {
            config: config.clone(),
            tfdb: ToolforgeDB::default(),
            sites_cache: vec![],
            site_matrix: SiteMatrix::new(&wikidata_api).await?,
        };
        info!("Baglama2::new: adding tooldb + Commons core/links MySQL pools");
        ret.tfdb.add_mysql_pool("tooldb", &config["tooldb"])?;
        ret.tfdb
            .add_mysql_pool(POOL_COMMONS_CORE, &config["commons"])?;
        // A missing `commons_links` entry must fail startup, not the first
        // links query, so say what to add.
        ret.tfdb
            .add_mysql_pool(POOL_COMMONS_LINKS, &config["commons_links"])
            .map_err(|e| {
                anyhow!(
                    "config needs a '{POOL_COMMONS_LINKS}' pool for the Commons links \
                     cluster (links.commonswiki…, the x4 split), e.g. \
                     `\"{POOL_COMMONS_LINKS}\": {{ \"url\": \"mysql://USER:PASS@links.commonswiki.web.db.svc.wikimedia.cloud:3306/commonswiki_p\" }}`: {e}"
                )
            })?;
        info!("Baglama2::new: populating sites cache from tool DB");
        ret.populate_sites().await?;
        info!("Baglama2::new: ready");
        Ok(ret)
    }

    pub fn sqlite_data_root_path(&self) -> String {
        self.config
            .get("sqlite_data_root_path")
            .expect("sqlite_data_root_path not found")
            .as_str()
            .expect("sqlite_data_root_path not found")
            .to_string()
    }

    /// Root directory of the gzb view-data files. Defaults to `gzb` inside
    /// the SQLite data root, which is where the PHP API looks.
    pub fn gzb_data_root_path(&self) -> std::path::PathBuf {
        match self
            .config
            .get("gzb_data_root_path")
            .and_then(|v| v.as_str())
        {
            Some(path) => path.into(),
            None => std::path::Path::new(&self.sqlite_data_root_path()).join("gzb"),
        }
    }

    /// The code a wiki has in the pageview dumps: its host name without
    /// `.org`, e.g. `enwiki` → `en.wikipedia`, `wikidatawiki` →
    /// `wikidata`. The site matrix comes first, because `sites.server` in
    /// the tool DB is wrong for several special wikis (`meta.wikipedia.org`);
    /// but the site matrix omits closed wikis, which still get views.
    pub fn wiki_dump_code(&self, wiki: &str) -> Option<String> {
        if let Ok(url) = self.site_matrix.get_server_url_for_wiki(wiki) {
            return Self::dump_code_from_server_url(&url);
        }
        let site = self
            .sites_cache
            .iter()
            .find(|s| s.giu_code().as_deref() == Some(wiki))?;
        Self::dump_code_from_server_url(site.server().as_deref()?)
    }

    /// The dump drops a leading `www.`: `www.wikidata.org` is `wikidata`.
    fn dump_code_from_server_url(url: &str) -> Option<String> {
        let host = url.split("://").last()?.trim_end_matches('/');
        let host = host.strip_prefix("www.").unwrap_or(host);
        host.strip_suffix(".org").map(|s| s.to_string())
    }

    pub async fn deactivate_nonexistent_categories(&self) -> Result<()> {
        let sql = format!(
            "{} WHERE is_user_name=0 AND is_active=1",
            RowGroup::sql_select()
        );
        info!("deactivate_nonexistent_categories: querying active groups from tool DB");
        let groups = self
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<RowGroup>)
            .await?;
        let active_categories = groups
            .iter()
            .map(|group| group.category().to_owned())
            .collect::<Vec<String>>();
        info!(
            "deactivate_nonexistent_categories: {} active categories; checking existence on Commons",
            active_categories.len()
        );
        let existing_categories = self.get_existing_categories(&active_categories).await?;
        info!(
            "deactivate_nonexistent_categories: {} of {} categories exist on Commons",
            existing_categories.len(),
            active_categories.len()
        );
        let non_existing_categories = active_categories
            .iter()
            .filter(|category| !existing_categories.contains(*category))
            .cloned()
            .collect::<Vec<String>>();
        let groups_to_deactivate = groups
            .iter()
            .filter(|group| non_existing_categories.contains(group.category()))
            .map(|group| group.id())
            .collect::<Vec<DbId>>();
        if groups_to_deactivate.is_empty() {
            return Ok(());
        }
        self.deactivate_groups(&groups_to_deactivate).await?;
        Ok(())
    }

    async fn deactivate_groups(&self, group_ids: &[DbId]) -> Result<()> {
        let placeholders = Baglama2::sql_placeholders(group_ids.len());
        let sql = format!("UPDATE `groups` SET is_active=0 WHERE id IN ({placeholders})");
        self.get_tooldb_conn()
            .await?
            .exec_drop(sql, group_ids.to_owned())
            .await?;
        Ok(())
    }

    async fn get_existing_categories(&self, categories: &[String]) -> Result<Vec<String>> {
        if categories.is_empty() {
            return Ok(vec![]);
        }
        let categories = categories
            .iter()
            .map(|category| category.replace(" ", "_"))
            .collect::<Vec<String>>();
        let placeholders = Baglama2::sql_placeholders(categories.len());
        let sql = format!("SELECT `page_title` FROM `page` WHERE `page_namespace`=14 AND `page_title` IN ({placeholders})");
        info!(
            "get_existing_categories: running single IN query against Commons `page` with {} placeholders",
            categories.len()
        );
        let results = self
            .get_commons_conn_for_tables(&["page"])
            .await?
            .exec_iter(sql, categories.to_owned())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        info!(
            "get_existing_categories: Commons query returned {} rows",
            results.len()
        );
        let results = results
            .iter()
            .map(|category| category.replace("_", " "))
            .collect::<Vec<String>>();
        Ok(results)
    }

    pub fn get_config_from_file(filename: &str) -> Result<serde_json::Value> {
        let path = if filename.starts_with('/') {
            Path::new(filename).to_path_buf()
        } else {
            let mut path = env::current_dir().expect("Can't get CWD");
            path.push(filename);
            path
        };
        let file = File::open(&path)?;
        Ok(serde_json::from_reader(file)?)
    }

    /// Acquire a pooled connection, retrying transient failures with backoff
    /// and capping each attempt so it can't block forever (the prod-on-
    /// Toolforge failure mode). Toolforge replicas occasionally refuse or are
    /// slow to hand out a connection; a single 30s cap with no retry was too
    /// brittle for a long-running batch job. Total budget is bounded:
    /// `DB_CONN_RETRIES` attempts of up to `DB_CONN_TIMEOUT` each, plus
    /// backoff, so it still fails loudly rather than hanging indefinitely.
    ///
    /// Refusals for `max_user_connections` don't count as attempts: they mean
    /// the tool's connections are all busy (possibly in another job), so this
    /// waits for one to free up, for up to `DB_CONN_LIMIT_WAIT`.
    async fn get_conn_with_timeout(&self, name: &'static str) -> Result<Conn> {
        let started = std::time::Instant::now();
        let mut failures = 0;
        let mut limit_waits = 0;
        loop {
            let err =
                match tokio::time::timeout(Self::DB_CONN_TIMEOUT, self.tfdb.get_connection(name))
                    .await
                {
                    Ok(Ok(conn)) => return Ok(conn),
                    Ok(Err(DatabaseError::MySql(e)))
                        if is_server_error(&e, ER_USER_LIMIT_REACHED) =>
                    {
                        if started.elapsed() >= Self::DB_CONN_LIMIT_WAIT {
                            return Err(anyhow!(
                                "Failed to acquire '{name}' database connection: still at the \
                             connection limit after {}s ({e})",
                                started.elapsed().as_secs()
                            ));
                        }
                        limit_waits += 1;
                        // 30s, doubling up to 5 min.
                        let wait = Duration::from_secs((15u64 << limit_waits.min(5)).min(300));
                        warn!(
                            "get_conn '{name}': at the connection limit; waiting {}s",
                            wait.as_secs()
                        );
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    Ok(Err(e)) => e.to_string(),
                    Err(_) => format!("timed out after {}s", Self::DB_CONN_TIMEOUT.as_secs()),
                };
            failures += 1;
            warn!(
                "get_conn '{name}': attempt {failures}/{} failed: {err}",
                Self::DB_CONN_RETRIES
            );
            if failures >= Self::DB_CONN_RETRIES {
                return Err(anyhow!(
                    "Failed to acquire '{name}' database connection after {} attempts ({err})",
                    Self::DB_CONN_RETRIES
                ));
            }
            // Linear-ish backoff: 3s, 6s, 9s, …
            tokio::time::sleep(Duration::from_secs(3 * failures as u64)).await;
        }
    }

    pub async fn get_tooldb_conn(&self) -> Result<Conn> {
        self.get_conn_with_timeout("tooldb").await
    }

    /// A connection to the Commons replica cluster that can serve a query
    /// reading all of `tables`.
    ///
    /// Commons had its links tables moved to a separate cluster (`x4`) in
    /// September 2026, so "the Commons connection" is no longer a single
    /// thing. The cluster is chosen with [`DbCluster::for_tables`] instead of
    /// being hard-coded, so a query keeps working when tables move again.
    ///
    /// Tables that no single cluster holds — a join across the split — are an
    /// error; such joins must be done in code.
    pub async fn get_commons_conn_for_tables(&self, tables: &[&str]) -> Result<Conn> {
        self.get_conn_with_timeout(Self::commons_pool_key_for_tables(tables)?)
            .await
    }

    /// The pool key serving a query over `tables` on Commons, resolved through
    /// [`DbCluster`].
    ///
    /// Kept pure so the table-to-cluster mapping can be unit-tested without a
    /// database; the `&'static str` is what `get_conn_with_timeout` needs.
    fn commons_pool_key_for_tables(tables: &[&str]) -> Result<&'static str> {
        Ok(Self::commons_pool_key(DbCluster::for_tables(
            COMMONS_WIKI,
            tables,
        )?))
    }

    /// Maps a Commons cluster to the pool key registered for it. `Core` and
    /// `Links` are the two clusters Commons has; a future split adds a pool
    /// key and an arm here.
    fn commons_pool_key(cluster: DbCluster) -> &'static str {
        match cluster {
            DbCluster::Core => POOL_COMMONS_CORE,
            DbCluster::Links => POOL_COMMONS_LINKS,
            // The term store is Wikidata-only, so `for_tables` never returns
            // it for Commons; fall back to core rather than inventing a pool.
            DbCluster::TermStore => POOL_COMMONS_CORE,
        }
    }

    async fn populate_sites(&mut self) -> Result<()> {
        let sql = "SELECT id,server,giu_code FROM `sites`";
        self.sites_cache = self
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

    pub fn value2opt_string(value: &mysql_async::Value) -> Option<String> {
        match value {
            mysql_async::Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        }
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
        Ok(())
    }

    async fn ensure_sites_in_tooldb(
        &self,
        sites: Vec<(String, String, String, String)>,
    ) -> Result<()> {
        let params = sites
            .iter()
            .flat_map(|(server, giu_code, project, language)| [server, giu_code, project, language])
            .collect::<Vec<_>>();
        let placeholder = "(?,?,?,?)".to_string();
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(sites.len(), placeholder);
        let placeholders = placeholders.join(",");
        let sql = format!(
            "INSERT IGNORE INTO `sites` (server,giu_code,project,language) VALUES {placeholders}"
        );
        self.get_tooldb_conn().await?.exec_drop(sql, params).await?;
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
            .get_commons_conn_for_tables(&["sites"])
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(String, String, String, String)>)
            .await?;
        Ok(sites)
    }

    // TESTED
    pub async fn get_group(&self, group_id: &GroupId) -> Result<Option<RowGroup>> {
        let sql = format!("{} WHERE id={group_id}", RowGroup::sql_select());
        let groups = self
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<RowGroup>)
            .await?;
        Ok(groups.first().map(|group| group.to_owned()))
    }

    // TESTED
    pub fn sql_placeholders(num: usize) -> String {
        let mut placeholders = "?,".repeat(num);
        placeholders.pop();
        placeholders
    }

    /// Runs a read query on the Commons cluster that holds `tables`, retrying
    /// failures, and returns all rows.
    ///
    /// Each attempt sets MariaDB's `max_statement_time`, so the server ends a
    /// query that runs too long and its connection is free again. A query the
    /// client merely gives up on keeps running on the server and keeps holding
    /// one of the tool's few connections, so retries would pile up until the
    /// server refuses new ones (`max_user_connections`). The client-side
    /// timeout is only a backstop, slightly longer than the server's limit.
    pub async fn query_commons<T, P>(&self, tables: &[&str], sql: &str, params: P) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Clone + Send,
    {
        let attempts = Self::DB_QUERY_TIME_LIMITS.len();
        let mut last_err = anyhow!("no attempt made");
        for (attempt, &limit) in Self::DB_QUERY_TIME_LIMITS.iter().enumerate() {
            if attempt > 0 {
                warn!(
                    "query_commons: attempt {attempt}/{attempts} failed ({last_err:#}); retrying"
                );
                self.hold_on().await;
            }
            let mut conn = match self.get_commons_conn_for_tables(tables).await {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            if let Err(e) = conn
                .query_drop(format!("SET SESSION max_statement_time={limit}"))
                .await
            {
                // Not MariaDB, or the connection is broken; the client-side
                // timeout still applies.
                warn!("query_commons: cannot set max_statement_time: {e}");
            }
            let query = async {
                let result = conn.exec_iter(sql, params.clone()).await?;
                result.map_and_drop(from_row::<T>).await
            };
            let client_limit = Duration::from_secs(limit) + Self::DB_QUERY_CLIENT_GRACE;
            match tokio::time::timeout(client_limit, query).await {
                Ok(Ok(rows)) => return Ok(rows),
                Ok(Err(e)) if is_server_error(&e, ER_STATEMENT_TIMEOUT) => {
                    last_err = anyhow!("Commons query exceeded the server's {limit}s limit");
                }
                Ok(Err(e)) => last_err = e.into(),
                Err(_) => {
                    // The connection is mid-query; don't hand it back to the pool.
                    tokio::spawn(conn.disconnect());
                    last_err = anyhow!(
                        "Commons query timed out client-side after {}s",
                        client_limit.as_secs()
                    );
                }
            }
        }
        Err(last_err.context(format!("Commons query failed after {attempts} attempts")))
    }

    // TESTED
    async fn find_subcats(&self, root: &[String], depth: isize) -> Result<Vec<String>> {
        let mut depth = depth;
        let mut check = root.to_owned();
        // Use a HashSet for O(1) membership tests; a Vec would give O(n) per check,
        // leading to O(n²) behaviour over deep category trees.
        let mut subcats: HashSet<String> = HashSet::new();
        loop {
            if depth == 0 {
                break;
            }
            // Keep only categories we haven't visited yet.
            let remaining: Vec<String> = check
                .into_iter()
                .filter(|category| !subcats.contains(category))
                .collect();
            if remaining.is_empty() {
                break;
            }
            subcats.extend(remaining.iter().cloned());
            let placeholders = Baglama2::sql_placeholders(remaining.len());
            let sql = format!(
                "SELECT DISTINCT FROM_BASE64(TO_BASE64(page_title))
	            FROM page,categorylinks,linktarget
	            WHERE page_id=cl_from
	            AND cl_target_id=lt_id AND lt_namespace=14
	            AND lt_title IN ({})
	            AND cl_type='subcat'",
                placeholders
            );
            check = self
                .query_commons(&["page", "categorylinks", "linktarget"], &sql, remaining)
                .await?;
            if check.is_empty() {
                break;
            }
            subcats.extend(check.iter().cloned());
            depth -= 1;
        }
        // Convert to a sorted Vec to match the previous behaviour (callers rely on
        // the result being usable as SQL IN-list parameters).
        let mut result: Vec<String> = subcats.into_iter().collect();
        result.sort();
        Ok(result)
    }

    // TESTED
    pub async fn get_pages_in_category(
        &self,
        category: &str,
        depth: isize,
        namespace: isize,
    ) -> Result<Vec<String>> {
        let category = category.replace(" ", "_");
        let categories = self
            .find_subcats(std::slice::from_ref(&category), depth)
            .await?;
        if namespace == 14 {
            return Ok(categories);
        }
        let mut ret = vec![];
        for cats in categories.chunks(1000) {
            let placeholders = Baglama2::sql_placeholders(cats.len());
            let sql = format!(
                "SELECT DISTINCT FROM_BASE64(TO_BASE64(page_title))
                FROM page,categorylinks,linktarget
                WHERE cl_from=page_id AND page_namespace={namespace}
                AND cl_target_id=lt_id AND lt_namespace=14
                AND lt_title IN ({})
                AND page_is_redirect=0",
                placeholders
            );
            let mut result = self
                .query_commons(
                    &["page", "categorylinks", "linktarget"],
                    &sql,
                    cats.to_vec(),
                )
                .await?;
            ret.append(&mut result);
        }
        ret.sort();
        ret.dedup();
        Ok(ret)
    }

    /// Gets all images uploaded by a user
    pub async fn get_files_from_user_name(&self, user_name: &str) -> Result<Vec<String>> {
        let sql = "SELECT DISTINCT FROM_BASE64(TO_BASE64(img_name)) FROM image,actor,user WHERE img_actor=actor_id AND user_name=:user_name AND user_id=actor_user";
        self.query_commons(
            &["image", "actor", "user"],
            sql,
            mysql_async::params! {user_name},
        )
        .await
    }

    pub async fn hold_on(&self) {
        let secs = self.config["hold_on"].as_u64().unwrap_or(5);
        // thread::sleep(time::Duration::from_secs(secs));
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value2opt_string_valid_utf8() {
        let value = mysql_async::Value::Bytes(b"k\xc3\xa9pe".to_vec()); // "képe" in UTF-8
        assert_eq!(Baglama2::value2opt_string(&value), Some("képe".to_string()));
    }

    #[test]
    fn test_value2opt_string_ascii() {
        let value = mysql_async::Value::Bytes(b"hello".to_vec());
        assert_eq!(
            Baglama2::value2opt_string(&value),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_value2opt_string_invalid_utf8_lossy() {
        // 0xff is not valid in any UTF-8 sequence; from_utf8_lossy replaces it with U+FFFD.
        let value = mysql_async::Value::Bytes(b"a\xffb".to_vec());
        assert_eq!(
            Baglama2::value2opt_string(&value),
            Some("a\u{FFFD}b".to_string())
        );
    }

    #[test]
    fn test_value2opt_string_non_bytes_returns_none() {
        let value = mysql_async::Value::NULL;
        assert_eq!(Baglama2::value2opt_string(&value), None);

        let value = mysql_async::Value::Int(42);
        assert_eq!(Baglama2::value2opt_string(&value), None);
    }

    #[test]
    fn test_value2opt_string_empty_bytes() {
        let value = mysql_async::Value::Bytes(vec![]);
        assert_eq!(Baglama2::value2opt_string(&value), Some(String::new()));
    }

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
    fn test_dump_code_from_server_url() {
        assert_eq!(
            Baglama2::dump_code_from_server_url("https://en.wikipedia.org"),
            Some("en.wikipedia".to_string())
        );
        assert_eq!(
            Baglama2::dump_code_from_server_url("https://www.wikidata.org/"),
            Some("wikidata".to_string())
        );
        assert_eq!(
            Baglama2::dump_code_from_server_url("https://example.com"),
            None
        );
    }

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(Baglama2::sql_placeholders(50).len(), 99);
    }

    /// The Commons links tables moved to their own cluster in September 2026;
    /// queries must be routed by the tables they read, not by a single
    /// hard-coded Commons pool.
    #[test]
    fn test_commons_pool_key_for_tables() {
        // `page` exists on both clusters; every other links table only on the
        // links cluster, so a join over them belongs there.
        assert_eq!(
            Baglama2::commons_pool_key_for_tables(&["page", "categorylinks", "linktarget"])
                .unwrap(),
            POOL_COMMONS_LINKS
        );
        assert_eq!(
            Baglama2::commons_pool_key_for_tables(&["globalimagelinks"]).unwrap(),
            POOL_COMMONS_LINKS
        );

        // Tables that stayed on the core cluster keep using the core pool.
        assert_eq!(
            Baglama2::commons_pool_key_for_tables(&["image", "actor", "user"]).unwrap(),
            POOL_COMMONS_CORE
        );
        assert_eq!(
            Baglama2::commons_pool_key_for_tables(&["sites"]).unwrap(),
            POOL_COMMONS_CORE
        );
        // `page` alone still resolves to core, the first cluster that holds it.
        assert_eq!(
            Baglama2::commons_pool_key_for_tables(&["page"]).unwrap(),
            POOL_COMMONS_CORE
        );

        // A query spanning the split cannot be served by one connection, so
        // it is an error rather than a silently wrong result.
        assert!(Baglama2::commons_pool_key_for_tables(&["actor", "pagelinks"]).is_err());
    }

    #[tokio::test]
    async fn test_get_sites() {
        let baglama = Baglama2::new().await.unwrap();
        let sites1 = baglama.get_sites().unwrap(); // Raw
        let sites2 = baglama.get_sites().unwrap(); // From cache
        assert_eq!(sites1.len(), sites2.len());
        assert!(sites1
            .iter()
            .any(|site| *site.server() == Some("zh-min-nan.wiktionary.org".to_string())));
    }

    #[tokio::test]
    async fn test_get_group() {
        let baglama = Baglama2::new().await.unwrap();
        let group = baglama
            .get_group(&1255.try_into().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            group.category(),
            "Images from Archives of Ontario – RG 14-100 Official Road Maps of Ontario"
        );
    }

    #[tokio::test]
    async fn test_get_pages_in_category() {
        let baglama = Baglama2::new().await.unwrap();
        let images = baglama
            .get_pages_in_category("Blue sky in Berlin", 3, 6)
            .await
            .unwrap();
        assert!(images.contains(&"2013-06-07_Kindergartenfest_Berlin-Karow_03.jpg".to_string()));
    }

    #[tokio::test]
    async fn test_get_files_from_user_name() {
        let baglama = Baglama2::new().await.unwrap();
        let files = baglama
            .get_files_from_user_name("Magnus Manske")
            .await
            .unwrap();
        assert!(files.contains(&"2002-07_Sylt_-_Westerland_(panorama).jpg".to_string()));
    }

    // `query_commons` relies on the replica ending queries itself.
    #[tokio::test]
    async fn test_commons_max_statement_time() {
        let baglama = Baglama2::new().await.unwrap();
        let mut conn = baglama
            .get_commons_conn_for_tables(&["page"])
            .await
            .unwrap();
        conn.query_drop("SET SESSION max_statement_time=1")
            .await
            .unwrap();
        let err = conn
            .query_drop("SELECT COUNT(*) FROM page a, page b")
            .await
            .unwrap_err();
        assert!(is_server_error(&err, ER_STATEMENT_TIMEOUT), "{err}");
    }

    #[tokio::test]
    async fn test_get_group_utf8() {
        let baglama = Baglama2::new().await.unwrap();
        let group = baglama
            .get_group(&292.try_into().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            group.category(),
            "Files of Museum für Kunst und Gewerbe Hamburg uploaded by RKBot"
        );
    }
}
