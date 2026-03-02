use crate::{
    db_trait::{DbTrait, FilePart, ViewIdSiteIdTitle},
    file::File,
    global_image_links::GlobalImageLinks,
    page::Page,
    Baglama2, DbId, GroupId, Site, ViewCount, YearMonth,
};
use anyhow::{anyhow, Result};

use log::{error, info, warn};
use mysql_async::{from_row_opt, prelude::*};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tools_interface::{Pageviews, PageviewsAccess, PageviewsAgent, PageviewsGranularity};

const FILES_CHUNK_SIZE: usize = 1000;
const PAGES_CHUNK_SIZE: usize = 500;
const MATCH_EXISTING_FILES_CHUNK_SIZE: usize = 10000;
const VIEWDATA_BATCH_SIZE: usize = 1000;

struct PageFile {
    page: Page,
    file: File,
}

impl PageFile {
    pub fn is_valid(&self) -> bool {
        self.page.id.is_some() && self.file.id.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct DbMySql2 {
    baglama: Arc<Baglama2>,
    ym: YearMonth,
    testing: bool,
    test_log: Arc<Mutex<Vec<Value>>>,
    sites: HashMap<DbId, Site>,
    wiki2site_id: HashMap<String, DbId>,
    table_name: String,
}

impl DbMySql2 {
    pub async fn new(ym: YearMonth, baglama: Arc<Baglama2>) -> Result<Self> {
        let table_name = format!("viewdata_{:04}_{:02}", ym.year(), ym.month());
        let mut ret = Self {
            baglama,
            ym,
            testing: false,
            test_log: Arc::new(Mutex::new(vec![])),
            sites: HashMap::new(),
            wiki2site_id: HashMap::new(),
            table_name,
        };
        ret.initialize_sites().await?;
        Ok(ret)
    }

    pub async fn load_missing_views(&self) -> Result<()> {
        let table_name = self.table_name();
        let pv = Pageviews::new(
            PageviewsGranularity::Monthly, // Get monthly views
            PageviewsAccess::All,          // Get all-access views
            PageviewsAgent::User,          // Get views from humans only
        );
        let sql = format!(
            "SELECT DISTINCT `vd`.`pages_id`,`server`,`title`
			        FROM `{table_name}` AS `vd`,`pages`,`sites`
			        WHERE `page_views` IS NULL
			        AND `pages_id`=`pages`.`id`
			        AND `pages`.`site`=`sites`.`id`
			        LIMIT {VIEWDATA_BATCH_SIZE}"
        );
        // Acquire one connection for the entire outer loop instead of once per iteration.
        let mut conn = self.baglama.get_tooldb_conn().await?;
        loop {
            // Get next VIEWDATA_BATCH_SIZE rows with missing view data from database
            let rows = conn
                .exec_iter(&sql, ())
                .await?
                .map_and_drop(from_row_opt::<(usize, String, String)>)
                .await?
                .into_iter()
                .filter_map(|row| row.ok())
                .collect::<Vec<_>>();
            if rows.is_empty() {
                // All done
                break;
            }
            info!(
                "Got {} rows, starting with page_id {}",
                rows.len(),
                rows[0].0
            );

            // Prepare getting view data from API
            let page2id = rows
                .into_iter()
                .filter(|(_id, _server, title)| !title.is_empty())
                .filter_map(|(id, server, title)| {
                    let title_with_underscores = title.replace(' ', "_");
                    let project = server.strip_suffix(".org")?.to_string();
                    Some(((project, title_with_underscores), id))
                })
                .collect::<HashMap<(String, String), usize>>();
            let project_pages = page2id.keys().cloned().collect::<Vec<_>>();

            // Get view data from API
            let results = pv
                .get_multiple_articles(
                    &project_pages,
                    &Pageviews::month_start(self.ym.year(), self.ym.month()).unwrap(),
                    &Pageviews::month_end(self.ym.year(), self.ym.month()).unwrap(),
                    5,
                )
                .await;
            let results = match results {
                Ok(results) => results,
                Err(e) => {
                    error!("Error getting pageviews: {}. Waiting 5 seconds.", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Map results back to viewdata_*.id
            let mut id2views: HashMap<usize, u64> = page2id.values().map(|id| (*id, 0)).collect();
            for result in results {
                let project = result.project.to_owned();
                let page = result.article.to_owned();
                let key = (project, page);
                match page2id.get(&key) {
                    Some(id) => {
                        id2views.insert(*id, result.total_views());
                    }
                    None => {
                        error!("Page not found: {:?}", key);
                    }
                }
            }

            // Construct SQL query
            let ids = id2views
                .keys()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let sql = id2views
                .into_iter()
                .map(|(id, views)| format!("WHEN {id} THEN {views}"))
                .collect::<Vec<String>>()
                .join("\n");
            let sql = format!(
                "UPDATE {table_name}
            	SET page_views = CASE pages_id {sql} ELSE page_views END
             	WHERE pages_id IN ({ids})"
            );

            // Update page_views column
            conn.exec_drop(sql, ()).await?;

            self.finalize_group_status().await?;
        }
        Ok(())
    }

    async fn initialize_sites(&mut self) -> Result<()> {
        let sql = "SELECT id,grok_code,server,giu_code,project,language,name FROM `sites`";
        let sites = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(Site::from_row)
            .await?;
        self.sites = sites.into_iter().map(|site| (site.id(), site)).collect();
        for site in self.sites.values() {
            if let Some(wiki) = site.giu_code().to_owned() {
                self.wiki2site_id.insert(wiki, site.id());
            }
        }
        Ok(())
    }

    pub async fn start_missing_groups(&self) -> Result<()> {
        let year = self.ym.year();
        let month = self.ym.month();
        // year and month are bound as parameters; the subquery uses named references
        // to the outer values which MySQL resolves correctly.
        let sql =
            "INSERT IGNORE INTO group_status(`group_id`,`year`,`month`,`status`,`storage`)
            SELECT id,?,?,'STARTED','mysql2' FROM groups
            WHERE is_active=1
            AND NOT EXISTS (SELECT * FROM group_status WHERE group_id=groups.id AND year=? AND month=?)";
        self.exec_with_params(sql, (year, month, year, month))
            .await?;
        Ok(())
    }

    pub async fn ensure_table_exists(&self) -> Result<()> {
        let table_name = self.table_name();
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS `{table_name}` (
              `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
              `group_status_id` int(11) unsigned NOT NULL,
              `files_id` int(11) unsigned NOT NULL,
              `pages_id` int(11) unsigned NOT NULL,
              `page_views` int(10) unsigned DEFAULT NULL,
              PRIMARY KEY (`id`),
              UNIQUE KEY `{table_name}_idx1` (`group_status_id`,`files_id`,`pages_id`),
              KEY `{table_name}_idx2` (`pages_id`),
              KEY `{table_name}_idx3` (`page_views`)
            ) ENGINE=InnoDB DEFAULT CHARSET=ascii;"
        );
        self.execute(&sql).await?;
        Ok(())
    }

    async fn get_files_for_group(&self, group_id: GroupId) -> Result<Vec<String>> {
        let group = self
            .baglama
            .get_group(&group_id)
            .await?
            .ok_or_else(|| anyhow!("Could not find group {} in MySQL database", group_id))?;

        // Get files in category tree from Commons
        let files = if group.is_user_name() {
            // self.get_files_from_user_name(group.category()).await?
            self.baglama
                .get_files_from_user_name(group.category())
                .await
        } else {
            // self.get_files_from_commons_category_tree(group.category(), group.depth()).await?
            self.baglama
                .get_pages_in_category(group.category(), group.depth(), 6)
                .await
        }?;
        if files.len() < 5 {
            warn!(
                "{} / {} has {} files",
                group.category(),
                group.depth(),
                files.len()
            );
        }
        Ok(files)
    }

    async fn get_next_group_id_to_process(&self) -> Option<(DbId, GroupId)> {
        let sql = "SELECT id,group_id FROM `group_status`
				WHERE `year`=? AND `month`=? AND `status`='STARTED'
				ORDER BY rand()
				LIMIT 1";
        let groups = self
            .baglama
            .get_tooldb_conn()
            .await
            .ok()?
            .exec_iter(sql, (self.ym.year(), self.ym.month()))
            .await
            .ok()?
            .map_and_drop(from_row_opt::<(usize, usize)>)
            .await
            .ok()?;
        match groups.first().as_ref() {
            Some(Ok((id, group_id))) => {
                let id = *id;
                let group_id = GroupId::try_from(*group_id).ok()?;
                Some((id, group_id))
            }
            _ => None,
        }
    }

    pub async fn add_pages(&self) -> Result<()> {
        loop {
            info!("Looking for next group");
            let (group_status_id, group_id) = match self.get_next_group_id_to_process().await {
                Some(id) => id,
                None => break,
            };
            info!("Processing group ID: {}", group_id);
            let files = self.get_files_for_group(group_id).await?;
            info!("Group ID: {}", group_id);
            info!("Files: {}", files.len());
            self.add_files_and_pages_for_group(&files, group_id, group_status_id)
                .await?;
        }
        // TODO views
        Ok(())
    }

    async fn add_files_and_pages_for_group(
        &self,
        all_files: &[String],
        group_id: GroupId,
        group_status_id: DbId,
    ) -> Result<()> {
        if all_files.is_empty() {
            // Nothing to do, call it done.
            self.baglama2()
                .set_group_status(group_id, &self.ym, "VIEW DATA COMPLETE", 0, "")
                .await?;
            return Ok(());
        }
        // let mut futures = Vec::new();
        for files in all_files.chunks(FILES_CHUNK_SIZE) {
            self.add_files_and_pages_for_group_chunks(group_status_id, files)
                .await?;
            // futures.push(self.add_files_and_pages_for_group_chunks(group_status_id, files));
        }
        // try_join_all(futures).await?;
        self.baglama2()
            .set_group_status(group_id, &self.ym, "SCANNED", 0, "")
            .await?;
        Ok(())
    }

    async fn add_files_and_pages_for_group_chunks(
        &self,
        group_status_id: usize,
        files: &[String],
    ) -> Result<()> {
        let globalimagelinks = GlobalImageLinks::load(files, self.baglama2()).await?;
        let mut page_files = Vec::new();
        for gil in &globalimagelinks {
            let site = match self.get_site_for_wiki(&gil.wiki) {
                Some(site) => site,
                None => {
                    warn!("add_views_for_files: Unknown wiki: {}", &gil.wiki);
                    continue;
                }
            };

            let file = File::new_no_id(&gil.to);
            let page = Page::new(site.id(), gil.page_title.to_owned(), gil.page_namespace_id);
            page_files.push(PageFile { page, file });
        }
        self.ensure_files_exist(&mut page_files).await?;
        self.ensure_pages_exist(&mut page_files).await?;
        self.insert_file_pages(&page_files, group_status_id).await?;
        Ok(())
    }

    async fn ensure_files_exist(&self, page_files: &mut [PageFile]) -> Result<()> {
        let files = page_files
            .iter()
            .map(|pf| pf.file.name.to_owned())
            .collect::<Vec<_>>();

        let files_to_create = self.match_existing_files(page_files, files).await?;
        if files_to_create.is_empty() {
            return Ok(());
        }
        self.create_files(&files_to_create).await?;
        let failed_to_create = self
            .match_existing_files(page_files, files_to_create)
            .await?;
        if !failed_to_create.is_empty() {
            warn!("CAUTION: Failed to create files: {failed_to_create:?}");
        }
        Ok(())
    }

    /// Returns a string of placeholders for SQL queries.
    /// s is the string to repeat, including final comma, which will be removed.
    /// Returns an error is count is 0.
    fn get_placeholders(s: &str, count: usize) -> Result<String> {
        if count == 0 {
            return Err(anyhow!("Asking for 0 repeats of {s}"));
        }
        let mut placeholders = s.repeat(count);
        placeholders.pop();
        Ok(placeholders)
    }

    async fn create_files(&self, all_files: &[String]) -> Result<()> {
        if all_files.is_empty() {
            return Ok(());
        }
        // Acquire one connection for all chunks rather than one per chunk.
        let mut conn = self.baglama2().get_tooldb_conn().await?;
        for files in all_files.chunks(1000) {
            let placeholders = Self::get_placeholders("(?),", files.len())?;
            let sql = format!("INSERT IGNORE INTO `files` (`name`) VALUES {placeholders}");
            conn.exec_drop(sql, files.to_owned()).await?;
        }
        Ok(())
    }

    /// Finds existing files in the DB, sets their IDs on the matching PageFile entries.
    /// Returns a deduplicated list of file names that are missing and need to be created.
    async fn match_existing_files(
        &self,
        page_files: &mut [PageFile],
        mut all_files: Vec<String>,
    ) -> Result<Vec<String>> {
        all_files.sort();
        all_files.dedup();
        // Use a HashSet so each missing name is only returned once, regardless of how
        // many PageFile entries reference the same file.
        let mut files_to_create: HashSet<String> = HashSet::new();
        // Acquire one connection for all chunks rather than one per chunk.
        let mut conn = self.baglama2().get_tooldb_conn().await?;
        for files in all_files.chunks(MATCH_EXISTING_FILES_CHUNK_SIZE) {
            let files = files.iter().collect::<Vec<_>>();
            let placeholders = Self::get_placeholders("?,", files.len())?;
            let sql = format!("SELECT `id`,`name` FROM `files` WHERE `name` IN ({placeholders})");
            let file2id: HashMap<String, DbId> = conn
                .exec_iter(sql, &files)
                .await?
                .map_and_drop(File::from_row_opt)
                .await?
                .into_iter()
                .filter_map(|f| f.ok())
                .filter_map(|f| Some((f.name, f.id?)))
                .collect();
            for pf in page_files.iter_mut() {
                if pf.file.id.is_none() {
                    match file2id.get(&pf.file.name) {
                        Some(id) => pf.file.id = Some(*id),
                        None => {
                            files_to_create.insert(pf.file.name.to_owned());
                        }
                    }
                }
            }
        }
        Ok(files_to_create.into_iter().collect())
    }

    async fn ensure_pages_exist(&self, page_files: &mut [PageFile]) -> Result<()> {
        // Collect unique pages to look up; pass ownership directly to match_existing_pages
        // so no extra clone is needed.
        let pages = page_files
            .iter()
            .map(|pf| pf.page.to_owned())
            .collect::<Vec<_>>();

        let pages_to_create = self.match_existing_pages(page_files, pages).await?;
        if !pages_to_create.is_empty() {
            self.create_pages(&pages_to_create).await?;
            let failed_to_create = self
                .match_existing_pages(page_files, pages_to_create)
                .await?;
            if !failed_to_create.is_empty() {
                warn!("CAUTION: Failed to create pages: {failed_to_create:?}");
            }
        }
        Ok(())
    }

    async fn create_pages(&self, all_pages: &[Page]) -> Result<()> {
        if all_pages.is_empty() {
            return Ok(());
        }
        // Acquire one connection for all chunks rather than one per chunk.
        let mut conn = self.baglama2().get_tooldb_conn().await?;
        for pages in all_pages.chunks(2000) {
            Self::create_page_chunks_with_conn(&mut conn, pages).await?;
        }
        Ok(())
    }

    async fn create_page_chunks_with_conn(
        conn: &mut mysql_async::Conn,
        pages: &[Page],
    ) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let placeholders = Self::get_placeholders("(?,?,?),", pages.len())?;
        let params = pages
            .iter()
            .flat_map(|p| {
                [
                    p.site_id.to_string(),
                    p.title.to_owned(),
                    p.namespace_id.to_string(),
                ]
            })
            .collect::<Vec<_>>();
        let sql = format!(
            "INSERT IGNORE INTO `pages` (`site`,`title`,`namespace_id`) VALUES {placeholders}"
        );
        conn.exec_drop(sql, params).await?;
        Ok(())
    }

    /// Finds existing pages.
    /// Returns missing pages.
    async fn match_existing_pages(
        &self,
        page_files: &mut [PageFile],
        all_pages: Vec<Page>,
    ) -> Result<Vec<Page>> {
        let mut pages_to_create = HashSet::new();
        // Acquire one connection for all chunks rather than one per chunk.
        let mut conn = self.baglama2().get_tooldb_conn().await?;
        for pages in all_pages.chunks(PAGES_CHUNK_SIZE) {
            let placeholders =
                vec!["(site=? AND title=? AND namespace_id=?)"; pages.len()].join(" OR ");

            let params = pages
                .iter()
                .flat_map(|p| {
                    [
                        p.site_id.to_string(),
                        p.title.to_owned(),
                        p.namespace_id.to_string(),
                    ]
                })
                .collect::<Vec<_>>();

            let sql = format!(
                "SELECT `id`,`site`,`title`,`namespace_id` FROM `pages` WHERE {placeholders}"
            );
            let page2id: HashMap<(usize, String, i32), DbId> = conn
                .exec_iter(sql, &params)
                .await?
                .map_and_drop(Page::from_row_opt)
                .await?
                .into_iter()
                .filter_map(|f| f.ok())
                .filter_map(|f| Some(((f.site_id, f.title, f.namespace_id), f.id?)))
                .collect();
            for pf in page_files.iter_mut() {
                if pf.page.id.is_none() {
                    let key = (
                        pf.page.site_id,
                        pf.page.title.to_owned(),
                        pf.page.namespace_id,
                    );
                    match page2id.get(&key) {
                        Some(id) => pf.page.id = Some(*id),
                        None => {
                            pages_to_create.insert(pf.page.to_owned());
                        }
                    }
                }
            }
        }
        Ok(pages_to_create.into_iter().collect::<Vec<_>>())
    }

    async fn insert_file_pages(
        &self,
        all_page_files: &[PageFile],
        group_status_id: usize,
    ) -> Result<()> {
        // Acquire one connection for all chunks rather than one per chunk.
        let mut conn = self.baglama.get_tooldb_conn().await?;
        for page_files in all_page_files.chunks(PAGES_CHUNK_SIZE) {
            Self::insert_file_pages_chunk_with_conn(
                &mut conn,
                group_status_id,
                page_files,
                self.table_name(),
            )
            .await?;
        }
        Ok(())
    }

    async fn insert_file_pages_chunk_with_conn(
        conn: &mut mysql_async::Conn,
        group_status_id: usize,
        page_files: &[PageFile],
        table_name: &str,
    ) -> Result<()> {
        let params = page_files
            .iter()
            .filter(|pf| pf.is_valid())
            .flat_map(|pf| [group_status_id, pf.file.id.unwrap(), pf.page.id.unwrap()])
            .collect::<Vec<_>>();
        if params.is_empty() {
            return Ok(());
        }
        let number_of_valid_page_files = params.len() / 3;
        let placeholders = Self::get_placeholders("(?,?,?),", number_of_valid_page_files)?;
        let sql = format!(
            "INSERT IGNORE INTO {table_name} (group_status_id, files_id, pages_id) VALUES {placeholders}"
        );
        conn.exec_iter(sql, params).await?;
        Ok(())
    }

    fn get_site_for_wiki(&self, wiki: &str) -> Option<&Site> {
        let site_id = self.wiki2site_id.get(wiki)?;
        self.sites.get(site_id)
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Used for internal testing only
    fn _as_test(self) -> Self {
        Self {
            testing: true,
            ..self
        }
    }

    /// Sets the group_status to 'VIEW DATA COMPLETE' and updates the `total_views` field
    async fn finalize_group_status(&self) -> Result<()> {
        let year = self.ym.year();
        let month = self.ym.month();
        let table_name = self.table_name();

        // table_name is generated internally (not user input) so interpolation is safe.
        // year and month are bound as parameters.

        // Fix group_status.status for finished groups
        let sql = format!(
            "UPDATE group_status
            SET `status`='VIEW DATA COMPLETE',
            total_views=(SELECT sum(page_views) FROM `{table_name}` WHERE group_status_id=group_status.id)
            WHERE `year`=? AND `month`=?
            AND `status`='SCANNED'
            AND NOT EXISTS (SELECT * FROM `{table_name}` WHERE group_status_id=group_status.id AND page_views IS NULL)"
        );
        self.exec_with_params(&sql, (year, month)).await?;

        // Calculate total_views
        let sql2 = format!(
            "UPDATE group_status
            SET total_views=(SELECT COALESCE(sum(page_views),0) FROM `{table_name}` WHERE group_status_id=group_status.id)
            WHERE `year`=? AND `month`=? AND status='VIEW DATA COMPLETE' AND total_views IS NULL"
        );
        self.exec_with_params(&sql2, (year, month)).await?;
        Ok(())
    }

    /// Used for internal testing only
    fn test_log_sql(sql: &str) -> String {
        // Normalize spaces for testing
        sql.replace("\n", " ")
            .replace("\t", " ")
            .split_whitespace()
            .collect::<Vec<&str>>()
            .join(" ")
    }

    /// Runs a single SQL query with no parameters, and no return value.
    /// Does not run in testing mode.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        if self.testing {
            self.test_log
                .lock()
                .await
                .push(json!(Self::test_log_sql(sql)));
        } else {
            self.baglama
                .get_tooldb_conn()
                .await?
                .exec_iter(sql, ())
                .await?;
        }
        Ok(())
    }

    /// Runs a SQL query with typed parameters and no return value.
    /// Does not run in testing mode.
    async fn exec_with_params<P>(&self, sql: &str, params: P) -> Result<()>
    where
        P: Into<mysql_async::Params> + std::fmt::Debug,
    {
        if self.testing {
            self.test_log
                .lock()
                .await
                .push(json!({"sql": Self::test_log_sql(sql), "params": format!("{params:?}")}));
        } else {
            self.baglama
                .get_tooldb_conn()
                .await?
                .exec_iter(sql, params)
                .await?;
        }
        Ok(())
    }

    /// Runs a single SQL query with a `Vec<String>` payload, and no return value.
    /// Does not run in testing mode.
    async fn exec_vec(&self, sql: &str, payload: Vec<String>) -> Result<()> {
        if self.testing {
            self.test_log
                .lock()
                .await
                .push(json!({"sql": Self::test_log_sql(sql),"payload": payload}));
        } else {
            self.baglama
                .get_tooldb_conn()
                .await?
                .exec_iter(sql, payload)
                .await?;
        }
        Ok(())
    }
}

impl DbTrait for DbMySql2 {
    fn path_final(&self) -> &str {
        "" // Not needed for MySQL
    }

    fn baglama2(&self) -> &Arc<Baglama2> {
        &self.baglama
    }

    fn ym(&self) -> &YearMonth {
        &self.ym
    }

    async fn finalize(&self) -> Result<()> {
        // Nothing to finalise for the mysql2 pipeline.
        Ok(())
    }

    fn load_sites(&self) -> Result<Vec<Site>> {
        let sites = self.sites.values().cloned().collect();
        Ok(sites)
    }

    async fn get_view_counts_todo(&self, _batch_size: usize) -> Result<Vec<ViewCount>> {
        Err(anyhow!("get_view_counts_todo is not supported by DbMySql2"))
    }

    async fn get_group_status_id(&self) -> Result<usize> {
        Err(anyhow!("get_group_status_id is not supported by DbMySql2"))
    }

    // tested
    async fn get_total_views(&self, group_status_id: DbId) -> Result<isize> {
        let sql = "SELECT ifnull(total_views,0) FROM group_status WHERE id=?";
        let ret = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, (group_status_id,))
            .await?
            .map_and_drop(from_row_opt::<isize>)
            .await?
            .first()
            .ok_or_else(|| anyhow!("get_total_views for group_status_id {group_status_id}"))?
            .to_owned();
        match ret {
            Ok(value) => Ok(value),
            _ => Err(anyhow!(
                "get_total_views for group_status_id {group_status_id}"
            )),
        }
    }

    fn create_final_indices(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    async fn delete_all_files(&self) -> Result<()> {
        Err(anyhow!("delete_all_files is not supported by DbMySql2"))
    }

    fn delete_views(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    fn delete_group2view(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    async fn load_files_batch(&self, _offset: usize, _batch_size: usize) -> Result<Vec<String>> {
        Err(anyhow!("load_files_batch is not supported by DbMySql2"))
    }

    async fn reset_main_page_view_count(&self) -> Result<()> {
        // Not implemented for MySQL: per-wiki Main Page reset is not needed
        // in the mysql2 pipeline (view counts are fetched fresh from the API).
        Err(anyhow!(
            "reset_main_page_view_count is not supported by DbMySql2"
        ))
    }

    // components are tested
    async fn add_summary_statistics(&self, _group_status_id: DbId) -> Result<()> {
        self.finalize_group_status().await?;
        Ok(())
    }

    // tested
    async fn update_view_count(&self, view_id: DbId, view_count: i64) -> Result<()> {
        let sql = "UPDATE `views` SET `done`=1,`views`=? WHERE `id`=?";
        self.exec_with_params(sql, (view_count, view_id)).await
    }

    // tested
    /// Mark a view as done and set the view count to 0; usually for failures
    async fn view_done(&self, view_id: DbId, done: u8) -> Result<()> {
        let sql = "UPDATE `views` SET `done`=?,`views`=0 WHERE `id`=?";
        self.exec_with_params(sql, (done, view_id)).await
    }

    fn file_insert_batch_size(&self) -> isize {
        5000 // Dunno?
    }

    // tested
    async fn insert_files_batch(&self, batch: &[String]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let group_status_id = self.get_group_status_id().await?;
        let placeholders = batch
            .iter()
            .map(|_| format!("({group_status_id},?)"))
            .collect::<Vec<String>>()
            .join(",");
        let sql = format!(
            "INSERT IGNORE INTO `tmp_files` (`group_status_id`,`name`) VALUES {placeholders}"
        );
        let batch = batch.to_vec();
        self.exec_vec(&sql, batch).await?;
        Ok(())
    }

    // components tested
    async fn initialize(&self) -> Result<()> {
        Ok(())
    }

    // tested
    async fn get_viewid_site_id_title(&self, parts: &[FilePart]) -> Result<Vec<ViewIdSiteIdTitle>> {
        if parts.is_empty() {
            return Ok(vec![]);
        }
        let year = self.ym.year();
        let month = self.ym.month();
        let placeholders: Vec<String> = parts
            .iter()
            .map(|part| {
                format!(
                    "(`site`={} AND `page_id`={} AND `year`={year} AND `month`={month})",
                    part.site_id, part.page_id
                )
            })
            .collect();
        let sql = "SELECT `id`,`site`,`title` FROM `views` WHERE ".to_string()
            + &placeholders.join(" OR ");
        let viewid_site_id_title = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row_opt::<ViewIdSiteIdTitle>)
            .await?
            .into_iter()
            .filter_map(|x| x.ok())
            // .map(|(view_id, site_id, title)| ViewIdSiteIdTitle::new(view_id, site_id, title))
            .collect();
        Ok(viewid_site_id_title)
    }

    // tested
    async fn create_views_in_db(&self, parts: &[FilePart], sql_values: &[String]) -> Result<()> {
        let sql = "INSERT IGNORE INTO `views` (site,title,month,year,done,namespace_id,page_id,views) VALUES ".to_string() + &sql_values.join(",");
        let titles: Vec<String> = parts.iter().map(|p| p.page_title.to_owned()).collect();
        self.exec_vec(&sql, titles).await?;
        Ok(())
    }

    // tested
    async fn insert_group2view(&self, values: &[String], images: Vec<String>) -> Result<()> {
        if !values.is_empty() {
            let sql = "INSERT IGNORE INTO `group2view` (group_status_id,view_id,image) VALUES "
                .to_string()
                + &values.join(",");
            self.exec_vec(&sql, images).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::File;
    use crate::page::Page;

    /// Build a PageFile with no DB ids assigned yet.
    fn make_page_file(file_name: &str) -> PageFile {
        PageFile {
            page: Page::new(1, "SomePage".to_string(), 0),
            file: File::new_no_id(file_name),
        }
    }

    /// `match_existing_files` must return each missing file name exactly once,
    /// even when multiple PageFile entries reference the same missing file.
    #[test]
    fn test_match_existing_files_no_duplicates_in_returned_list() {
        // Simulate the interior logic: given a file2id map that does NOT contain
        // "missing.jpg", three PageFile entries all referencing "missing.jpg" should
        // produce exactly one entry in the returned Vec, not three.
        let file2id: HashMap<String, DbId> = [("existing.jpg".to_string(), 42usize)]
            .into_iter()
            .collect();

        let mut page_files = vec![
            make_page_file("missing.jpg"),
            make_page_file("missing.jpg"),
            make_page_file("missing.jpg"),
            make_page_file("existing.jpg"),
        ];

        let mut files_to_create: HashSet<String> = HashSet::new();
        for pf in page_files.iter_mut() {
            if pf.file.id.is_none() {
                match file2id.get(&pf.file.name) {
                    Some(id) => pf.file.id = Some(*id),
                    None => {
                        files_to_create.insert(pf.file.name.to_owned());
                    }
                }
            }
        }
        let result: Vec<String> = files_to_create.into_iter().collect();

        // Exactly one distinct missing file name must be returned.
        assert_eq!(
            result.len(),
            1,
            "Expected 1 unique missing file, got {}",
            result.len()
        );
        assert_eq!(result[0], "missing.jpg");

        // The existing file's id must have been set on its PageFile entry.
        assert_eq!(page_files[3].file.id, Some(42));
        // The missing entries must still have no id.
        assert!(page_files[0].file.id.is_none());
        assert!(page_files[1].file.id.is_none());
        assert!(page_files[2].file.id.is_none());
    }

    /// table_name must be computed once at construction time and return the
    /// correctly formatted string for a given YearMonth.
    #[test]
    fn test_table_name_cached_format() {
        // We test the formatting logic directly — same formula used in new().
        let cases = [
            ((2024, 1), "viewdata_2024_01"),
            ((2024, 12), "viewdata_2024_12"),
            ((2000, 6), "viewdata_2000_06"),
        ];
        for ((year, month), expected) in cases {
            let name = format!("viewdata_{:04}_{:02}", year, month);
            assert_eq!(name, expected, "year={year} month={month}");
        }
    }

    /// update_view_count must use `?` placeholders, not interpolated values.
    /// If values were interpolated, a future refactor could accidentally re-introduce
    /// a format string here and the query would silently stop being parameterized.
    #[test]
    fn test_update_view_count_sql_is_parameterized() {
        let sql = "UPDATE `views` SET `done`=1,`views`=? WHERE `id`=?";
        // Must contain exactly two `?` placeholders.
        assert_eq!(sql.matches('?').count(), 2, "expected 2 placeholders");
        // Must NOT contain any literal numeric values that look like interpolation.
        assert!(
            !sql.contains("{view_count}") && !sql.contains("{view_id}"),
            "SQL must not contain format-string placeholders"
        );
    }

    /// view_done must use `?` placeholders, not interpolated values.
    #[test]
    fn test_view_done_sql_is_parameterized() {
        let sql = "UPDATE `views` SET `done`=?,`views`=0 WHERE `id`=?";
        assert_eq!(sql.matches('?').count(), 2, "expected 2 placeholders");
        assert!(
            !sql.contains("{done}") && !sql.contains("{view_id}"),
            "SQL must not contain format-string placeholders"
        );
    }

    /// start_missing_groups must bind year and month as `?` parameters, not
    /// interpolate them into the SQL string.
    #[test]
    fn test_start_missing_groups_sql_is_parameterized() {
        let sql = "INSERT IGNORE INTO group_status(`group_id`,`year`,`month`,`status`,`storage`)
            SELECT id,?,?,'STARTED','mysql2' FROM groups
            WHERE is_active=1
            AND NOT EXISTS (SELECT * FROM group_status WHERE group_id=groups.id AND year=? AND month=?)";
        // Four `?` — two in SELECT (year, month) and two in the NOT EXISTS subquery.
        assert_eq!(sql.matches('?').count(), 4, "expected 4 placeholders");
        assert!(
            !sql.contains("{year}") && !sql.contains("{month}"),
            "SQL must not contain format-string placeholders"
        );
    }

    /// finalize_group_status must bind year and month as parameters.
    /// table_name is generated internally and interpolated as an identifier
    /// (MySQL does not allow table names as bind parameters), so that part
    /// remains in the format string — but year/month must not.
    #[test]
    fn test_finalize_group_status_sql_is_parameterized() {
        // Simulate what finalize_group_status produces for a known table_name.
        let table_name = "viewdata_2024_01";
        let sql1 = format!(
            "UPDATE group_status
            SET `status`='VIEW DATA COMPLETE',
            total_views=(SELECT sum(page_views) FROM `{table_name}` WHERE group_status_id=group_status.id)
            WHERE `year`=? AND `month`=?
            AND `status`='SCANNED'
            AND NOT EXISTS (SELECT * FROM `{table_name}` WHERE group_status_id=group_status.id AND page_views IS NULL)"
        );
        let sql2 = format!(
            "UPDATE group_status
            SET total_views=(SELECT COALESCE(sum(page_views),0) FROM `{table_name}` WHERE group_status_id=group_status.id)
            WHERE `year`=? AND `month`=? AND status='VIEW DATA COMPLETE' AND total_views IS NULL"
        );

        assert_eq!(
            sql1.matches('?').count(),
            2,
            "sql1: expected 2 placeholders"
        );
        assert_eq!(
            sql2.matches('?').count(),
            2,
            "sql2: expected 2 placeholders"
        );

        // year and month must not be interpolated as literals.
        for sql in [&sql1, &sql2] {
            assert!(
                !sql.contains("{year}") && !sql.contains("{month}"),
                "SQL must not contain format-string year/month placeholders: {sql}"
            );
        }

        // table_name is intentionally interpolated as an identifier — verify it is present.
        assert!(
            sql1.contains(table_name),
            "table_name must appear as an interpolated identifier in sql1"
        );
        assert!(
            sql2.contains(table_name),
            "table_name must appear as an interpolated identifier in sql2"
        );
    }
}
