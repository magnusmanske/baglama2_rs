use crate::{
    db_trait::{DbTrait, FilePart, ViewIdSiteIdTitle},
    file::File,
    global_image_links::GlobalImageLinks,
    page::Page,
    Baglama2, DbId, GroupId, Site, ViewCount, YearMonth,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use log::{error, warn};
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
}

impl DbMySql2 {
    pub async fn new(ym: YearMonth, baglama: Arc<Baglama2>) -> Result<Self> {
        let mut ret = Self {
            baglama,
            ym,
            testing: false,
            test_log: Arc::new(Mutex::new(vec![])),
            sites: HashMap::new(),
            wiki2site_id: HashMap::new(),
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
            "SELECT DISTINCT `vd`.`pages_id`,`server`,coalesce(CONVERT(`title` USING utf8),'') AS `title`
			        FROM `{table_name}` AS `vd`,`pages`,`sites`
			        WHERE `page_views` IS NULL
			        AND `pages_id`=`pages`.`id`
			        AND `pages`.`site`=`sites`.`id`
			        LIMIT {VIEWDATA_BATCH_SIZE}"
        );
        loop {
            // Get next VIEWDATA_BATCH_SIZE rows with missing view data from database
            let rows = self
                .baglama
                .get_tooldb_conn()
                .await?
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
            println!(
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
            self.baglama2()
                .get_tooldb_conn()
                .await?
                .exec_drop(sql, ())
                .await?;

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
        let sql = format!(
            "INSERT IGNORE INTO group_status(`group_id`,`year`,`month`,`status`,`storage`)
            SELECT id,{year},{month},'STARTED','mysql2' FROM groups
            WHERE is_active=1
            AND NOT EXISTS (SELECT * FROM group_status WHERE group_id=groups.id AND year={year} AND month={month})"
        );
        self.execute(&sql).await?;
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
        let sql = format!(
            "SELECT id,group_id FROM `group_status`
				WHERE `year`={} AND `month`={} AND `status`='STARTED'
				ORDER BY rand()
				LIMIT 1",
            self.ym.year(),
            self.ym.month()
        );
        let groups = self
            .baglama
            .get_tooldb_conn()
            .await
            .ok()?
            .exec_iter(sql, ())
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
            println!("Looking for next group");
            let (group_status_id, group_id) = match self.get_next_group_id_to_process().await {
                Some(id) => id,
                None => break,
            };
            println!("Processing group ID: {}", group_id);
            let files = self.get_files_for_group(group_id).await?;
            println!("Group ID: {}", group_id);
            println!("Files: {}", files.len());
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
                    println!("add_views_for_files: Unknown wiki: {}", &gil.wiki);
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
        for files in all_files.chunks(1000) {
            let placeholders = Self::get_placeholders("(?),", files.len())?;
            let sql = format!("INSERT IGNORE INTO `files` (`name`) VALUES {placeholders}");
            self.baglama2()
                .get_tooldb_conn()
                .await?
                .exec_drop(sql, files.to_owned())
                .await?;
        }
        Ok(())
    }

    /// Finds existing files.
    /// Returns missing files.
    async fn match_existing_files(
        &self,
        page_files: &mut [PageFile],
        mut all_files: Vec<String>,
    ) -> Result<Vec<String>> {
        all_files.sort();
        all_files.dedup();
        let mut file_to_create = Vec::new();
        for files in all_files.chunks(MATCH_EXISTING_FILES_CHUNK_SIZE) {
            let files = files.iter().collect::<Vec<_>>();
            let placeholders = Self::get_placeholders("?,", files.len())?;
            let sql = format!("SELECT `id`,`name` FROM `files` WHERE `name` IN ({placeholders})");
            let file2id: HashMap<String, DbId> = self
                .baglama2()
                .get_tooldb_conn()
                .await?
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
                            file_to_create.push(pf.file.name.to_owned());
                        }
                    }
                }
            }
        }
        Ok(file_to_create)
    }

    async fn ensure_pages_exist(&self, page_files: &mut [PageFile]) -> Result<()> {
        let pages = page_files
            .iter()
            .map(|pf| pf.page.to_owned())
            .collect::<Vec<_>>();

        let pages_to_create = self
            .match_existing_pages(page_files, pages.to_owned())
            .await?;
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
        // let mut futures = vec![];
        for pages in all_pages.chunks(2000) {
            self.create_page_chunks(pages).await?;
            // let future = self.create_page_chunks(pages);
            // futures.push(future);
        }
        // try_join_all(futures).await?;
        Ok(())
    }

    async fn create_page_chunks(&self, pages: &[Page]) -> Result<()> {
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
        self.baglama2()
            .get_tooldb_conn()
            .await?
            .exec_drop(sql, params)
            .await?;
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
        for pages in all_pages.chunks(PAGES_CHUNK_SIZE) {
            let placeholder = "(site=? AND title=? AND namespace_id=?)".to_string();
            let mut placeholders: Vec<String> = Vec::new();
            placeholders.resize(pages.len(), placeholder);
            let placeholders = placeholders.join(" OR ");

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
            let page2id: HashMap<(usize, String, i32), DbId> = self
                .baglama2()
                .get_tooldb_conn()
                .await?
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
        // let mut futures = vec![];
        for page_files in all_page_files.chunks(PAGES_CHUNK_SIZE) {
            self.insert_file_pages_chunk(group_status_id, page_files)
                .await?;
            // futures.push(self.insert_file_pages_chunk(group_status_id, page_files));
        }
        // try_join_all(futures).await?;
        Ok(())
    }

    async fn insert_file_pages_chunk(
        &self,
        group_status_id: usize,
        page_files: &[PageFile],
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
            "INSERT IGNORE INTO {} (group_status_id, files_id, pages_id) VALUES {placeholders}",
            self.table_name()
        );
        self.baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, params)
            .await?;
        Ok(())
    }

    fn get_site_for_wiki(&self, wiki: &str) -> Option<&Site> {
        let site_id = self.wiki2site_id.get(wiki)?;
        self.sites.get(site_id)
    }

    fn table_name(&self) -> String {
        format!("viewdata_{:04}_{:02}", self.ym.year(), self.ym.month())
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

        // Fix group_status.status for finished groups
        let sql = format!(
                "UPDATE group_status
                SET `status`='VIEW DATA COMPLETE',
                total_views=(SELECT sum(page_views) FROM `{table_name}` WHERE group_status_id=group_status.id)
                WHERE `year`={year} AND `month`={month}
                AND `status`='SCANNED'
                AND NOT EXISTS (SELECT * FROM `{table_name}` WHERE group_status_id=group_status.id AND page_views IS NULL);"

            );
        self.execute(&sql).await?;

        // Calculate total_views
        let sql2 = format!("UPDATE group_status
        	SET total_views=(SELECT COALESCE(sum(page_views),0) FROM `{table_name}` WHERE group_status_id=group_status.id)
         WHERE `year`={year} AND `month`={month} AND status='VIEW DATA COMPLETE' AND total_views IS NULL"
        );
        self.execute(&sql2).await?;
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
    /// Does not run in testing mode
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

    /// Runs a single SQL query with a `Vec<String>` payload, and no return value
    /// Does not run in testing mode
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

#[async_trait]
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
        self.delete_all_files().await?;
        Ok(())
    }

    fn load_sites(&self) -> Result<Vec<Site>> {
        let sites = self.sites.values().cloned().collect();
        Ok(sites)
    }

    // tested
    async fn get_view_counts_todo(&self, _batch_size: usize) -> Result<Vec<ViewCount>> {
        unimplemented!()
    }

    // tested
    async fn get_group_status_id(&self) -> Result<usize> {
        unimplemented!()
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

    // tested
    async fn delete_all_files(&self) -> Result<()> {
        unimplemented!()
    }

    fn delete_views(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    fn delete_group2view(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    // tested
    async fn load_files_batch(&self, _offset: usize, _batch_size: usize) -> Result<Vec<String>> {
        unimplemented!()
    }

    // tested
    async fn reset_main_page_view_count(&self) -> Result<()> {
        // // TODO for all wikis?
        // let main_page_id = 47751469;
        // let enwiki = 158;
        // let group_status_id = self.get_group_status_id().await?;
        // let sql = format!("SELECT `view_id` FROM `group2view` WHERE `group_status_id`={group_status_id} AND `view_id`=`views`.`id`");
        // let sql =
        //     format!("UPDATE views SET views=0 WHERE page_id={main_page_id} AND site={enwiki} AND views.id IN ({sql})");
        // self.execute(&sql).await
        unimplemented!()
    }

    // components are tested
    async fn add_summary_statistics(&self, _group_status_id: DbId) -> Result<()> {
        self.finalize_group_status().await?;
        Ok(())
    }

    // tested
    async fn update_view_count(&self, view_id: DbId, view_count: i64) -> Result<()> {
        let sql = format!("UPDATE `views` SET `done`=1,`views`={view_count} WHERE `id`={view_id}");
        self.execute(&sql).await
    }

    // tested
    /// Mark a view as done and set the view count to 0; usually for failures
    async fn view_done(&self, view_id: DbId, done: u8) -> Result<()> {
        let sql = format!("UPDATE `views` SET `done`={done},`views`=0 WHERE `id`={view_id}");
        self.execute(&sql).await
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
            + &placeholders.join(" OR ").to_string();
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
                + &values.join(",").to_string();
            self.exec_vec(&sql, images).await?;
        }
        Ok(())
    }
}
