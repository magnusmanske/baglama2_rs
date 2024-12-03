use crate::row_group::RowGroup;
use crate::row_group_status::RowGroupStatus;
use crate::GroupId;
use crate::Site;
use crate::YearMonth;
use anyhow::Result;
use core::time::Duration;
use lazy_static::lazy_static;
use mysql_async::{from_row, prelude::*, Conn};
use regex::Regex;
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use wikimisc::mediawiki::Api;
use wikimisc::site_matrix::SiteMatrix;
use wikimisc::toolforge_db::ToolforgeDB;

lazy_static! {
    static ref RE_WEBSERVER_WIKIPEDIA: Regex = Regex::new(r"^(.+)wiki$").expect("Regex error");
    static ref RE_WEBSERVER_WIKI: Regex = Regex::new(r"^(.+)(wik.+)$").expect("Regex error");
}

#[derive(Debug)]
pub struct Baglama2 {
    config: Value,
    tfdb: ToolforgeDB,
    apis: Arc<Mutex<HashMap<String, Api>>>,
    sites_cache: Vec<Site>,
    site_matrix: SiteMatrix,
}

impl Baglama2 {
    pub async fn new() -> Result<Self> {
        let config = match Self::get_config_from_file("config.json") {
            Ok(config) => config,
            Err(_) => {
                Self::get_config_from_file("/data/project/glamtools/baglama2_rs/config.json")?
            }
        };
        let wikidata_api = Api::new("https://www.wikidata.org/w/api.php").await?;
        let mut ret = Self {
            config: config.clone(),
            tfdb: ToolforgeDB::default(),
            apis: Arc::new(Mutex::new(HashMap::new())),
            sites_cache: vec![],
            site_matrix: SiteMatrix::new(&wikidata_api).await?,
        };
        ret.tfdb.add_mysql_pool("tooldb", &config["tooldb"])?;
        ret.tfdb.add_mysql_pool("commons", &config["commons"])?;
        ret.populate_sites().await?;
        Ok(ret)
    }

    async fn api(&self, wiki: &str) -> Option<Api> {
        match self.apis.lock().await.entry(wiki.to_string()) {
            Entry::Occupied(e) => {
                let api: &Api = e.get();
                Some(api.clone())
            }
            Entry::Vacant(entry) => {
                let namespaces = self.add_api(wiki).await?;
                let api = entry.insert(namespaces);
                Some(api.clone())
            }
        }
    }

    async fn add_api(&self, wiki: &str) -> Option<Api> {
        let server = self.site_matrix.get_server_url_for_wiki(wiki).ok()?;
        let url = format!("{server}/w/api.php");
        let api = Api::new(&url).await.ok()?;
        Some(api)
    }

    pub fn config(&self) -> &Value {
        &self.config
    }

    pub fn sqlite_schema_file(&self) -> String {
        self.config
            .get("sqlite_schema_file")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    pub fn sqlite_data_root_path(&self) -> String {
        self.config
            .get("sqlite_data_root_path")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn deactivate_nonexistent_categories(&self) -> Result<()> {
        let sql = format!(
            "{} WHERE is_user_name=0 AND is_active=1",
            RowGroup::sql_select()
        );
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
        let existing_categories = self.get_existing_categories(&active_categories).await?;
        let non_existing_categories = active_categories
            .iter()
            .filter(|category| !existing_categories.contains(*category))
            .cloned()
            .collect::<Vec<String>>();
        let groups_to_deactivate = groups
            .iter()
            .filter(|group| non_existing_categories.contains(group.category()))
            .map(|group| group.id())
            .collect::<Vec<usize>>();
        if groups_to_deactivate.is_empty() {
            return Ok(());
        }
        self.deactivate_groups(&groups_to_deactivate).await?;
        Ok(())
    }

    async fn deactivate_groups(&self, group_ids: &[usize]) -> Result<()> {
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
        let results = self
            .get_commons_conn()
            .await?
            .exec_iter(sql, categories.to_owned())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
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

    pub async fn get_tooldb_conn(&self) -> Result<Conn> {
        self.tfdb.get_connection("tooldb").await
    }

    pub async fn get_commons_conn(&self) -> Result<Conn> {
        self.tfdb.get_connection("commons").await
    }

    async fn populate_sites(&mut self) -> Result<()> {
        let sql = "SELECT id,grok_code,server,giu_code,project,language,name FROM `sites`";
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

    // UNTESTED
    pub fn value2opt_string(value: &mysql_async::Value) -> Option<String> {
        match value {
            mysql_async::Value::Bytes(bytes) => String::from_utf8(bytes.to_owned()).ok(),
            _ => None,
        }
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
    pub async fn get_group_status(
        &self,
        group_id: &GroupId,
        ym: &YearMonth,
    ) -> Result<Option<RowGroupStatus>> {
        let sql = "SELECT id,group_id,year,month,status,total_views,file,sqlite3 FROM `group_status` WHERE group_id=? AND year=? AND month=?" ;
        let sites: Vec<RowGroupStatus> = self
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, (group_id.as_usize(), ym.year(), ym.month()))
            .await?
            .map_and_drop(from_row::<RowGroupStatus>)
            .await?;
        let ret = sites.first().map(|x| x.to_owned());
        Ok(ret)
    }

    async fn get_namespace_prefix(&self, wiki: &str, namespace_id: i32) -> Option<String> {
        self.api(wiki)
            .await?
            .get_canonical_namespace_name(namespace_id.into())
            .map(|s| s.to_string())
    }

    // TESTED
    pub async fn prefix_with_namespace(
        &self,
        title: &str,
        namespace_id: i32,
        wiki: &str,
    ) -> Option<String> {
        let prefix = self.get_namespace_prefix(wiki, namespace_id).await?;
        if prefix.is_empty() {
            Some(title.to_string())
        } else {
            Some(format!("{prefix}:{title}"))
        }
    }

    // TESTED
    pub fn sql_placeholders(num: usize) -> String {
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(num, "?".to_string());
        placeholders.join(",")
    }

    async fn query_commons_repeat(&self, sql: &str, todo: &[String]) -> Result<Vec<String>> {
        let mut attempts_left = 5;
        let mut ret = vec![];
        loop {
            if attempts_left == 0 {
                break;
            }
            attempts_left -= 1;
            let mut conn = match self.get_commons_conn().await {
                Ok(conn) => conn,
                Err(e) => {
                    if attempts_left == 0 {
                        return Err(e);
                    } else {
                        continue;
                    }
                }
            };
            let result = match conn.exec_iter(sql, todo.to_owned()).await {
                Ok(res) => res,
                Err(e) => {
                    if attempts_left == 0 {
                        return Err(e.into());
                    } else {
                        drop(conn);
                        continue;
                    }
                }
            };
            ret = match result.map_and_drop(from_row::<String>).await {
                Ok(ret) => ret,
                Err(e) => {
                    if attempts_left == 0 {
                        return Err(e.into());
                    } else {
                        drop(conn);
                        continue;
                    }
                }
            }
        }
        Ok(ret)
    }

    // TESTED
    async fn find_subcats(&self, root: &Vec<String>, depth: usize) -> Result<Vec<String>> {
        let mut depth = depth;
        let mut check = root.to_owned();
        let mut subcats: Vec<String> = vec![];
        loop {
            if depth == 0 {
                break;
            }
            let todo: Vec<String> = check
                .iter()
                .filter(|category| !subcats.contains(category))
                .map(|category| category.to_owned())
                .collect();
            if todo.is_empty() {
                break;
            }
            subcats.extend_from_slice(&todo);
            let placeholders = Baglama2::sql_placeholders(todo.len());
            let sql = format!("SELECT DISTINCT FROM_BASE64(TO_BASE64(page_title)) FROM page,categorylinks WHERE page_id=cl_from AND cl_to IN ({}) AND cl_type='subcat'",placeholders);
            check = self.query_commons_repeat(&sql, &todo).await?;
            if check.is_empty() {
                break;
            }
            subcats.extend_from_slice(&check);
            subcats.sort();
            subcats.dedup();
            depth -= 1;
        }
        Ok(subcats)
    }

    // TESTED
    pub async fn get_pages_in_category(
        &self,
        category: &str,
        depth: usize,
        namespace: isize,
    ) -> Result<Vec<String>> {
        let category = category.replace(" ", "_");
        let categories = self.find_subcats(&vec![category.clone()], depth).await?;
        if namespace == 14 {
            return Ok(categories);
        }
        let mut ret = vec![];
        for cats in categories.chunks(1000) {
            let placeholders = Baglama2::sql_placeholders(cats.len());
            let sql = format!("SELECT DISTINCT FROM_BASE64(TO_BASE64(page_title)) FROM page,categorylinks WHERE cl_from=page_id AND page_namespace={namespace} AND cl_to IN ({}) AND page_is_redirect=0",placeholders);
            let mut result = self.query_commons_repeat(&sql, cats).await?;
            ret.append(&mut result);
        }
        ret.sort();
        ret.dedup();
        Ok(ret)
    }

    /// Gets all images uploaded by a user
    pub async fn get_files_from_user_name(&self, user_name: &str) -> Result<Vec<String>> {
        let sql = "SELECT DISTINCT FROM_BASE64(TO_BASE64(img_name)) FROM image,actor,user WHERE img_actor=actor_id AND user_name=:user_name AND user_id=actor_user";
        let mut conn = self.get_commons_conn().await?;
        let results = conn
            .exec_iter(sql, mysql_async::params! {user_name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(results)
    }

    // TESTED
    pub async fn get_next_group_id(
        &self,
        year: i32,
        month: u32,
        requires_previous_date: bool,
    ) -> Option<usize> {
        let mut conn = self.get_tooldb_conn().await.ok()?;
        let mut sql = "SELECT id FROM groups WHERE is_active=1".to_string();
        sql += " AND NOT EXISTS (SELECT * FROM group_status WHERE groups.id=group_id AND year=:year AND month=:month)";
        // Backfilling
        if requires_previous_date {
            sql += " AND EXISTS (SELECT * FROM group_status WHERE groups.id=group_id AND (year<:year OR (year=:year AND month<:month)))";
        }
        sql += " ORDER BY rand() LIMIT 1";
        let results = conn
            .exec_iter(sql, mysql_async::params!(year, month))
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?;
        results.first().map(|id| id.to_owned())
    }

    pub async fn clear_incomplete_group_status(&self, year: i32, month: u32) -> Result<()> {
        let sql = "DELETE FROM group_status WHERE year=:year AND month=:month AND status!='VIEW DATA COMPLETE'" ;
        self.get_tooldb_conn()
            .await?
            .exec_drop(sql, mysql_async::params! {year,month})
            .await?;
        Ok(())
    }

    pub async fn hold_on(&self) {
        let secs = self.config["hold_on"].as_u64().unwrap_or(5);
        // thread::sleep(time::Duration::from_secs(secs));
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }

    pub async fn set_group_status(
        &self,
        group_id: GroupId,
        ym: &YearMonth,
        status: &str,
        total_views: usize,
        sqlite_filename: &str,
    ) -> Result<()> {
        let mut mysql_connection = self.get_tooldb_conn().await?;
        let group_id = group_id.as_usize();
        let year = ym.year();
        let month = ym.month();
        let sql = "REPLACE INTO `group_status` (group_id,year,month,status,total_views,sqlite3) VALUES (:group_id,:year,:month,:status,:total_views,:sqlite_filename)";
        let _ = mysql_connection
            .exec_drop(
                sql,
                mysql_async::params! {group_id,year,month,status,total_views,sqlite_filename},
            )
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(Baglama2::sql_placeholders(50).len(), 99);
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
    async fn test_get_next_group_id() {
        let baglama = Baglama2::new().await.unwrap();
        let ng = baglama.get_next_group_id(2014, 1, false).await;
        assert!(ng.is_some());
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

    #[tokio::test]
    async fn test_prefix_with_namespace() {
        let baglama = Baglama2::new().await.unwrap();
        assert_eq!(
            baglama
                .prefix_with_namespace("Magnus Manske", 2, "enwiki")
                .await
                .unwrap(),
            "User:Magnus Manske".to_string()
        );
    }

    #[tokio::test]
    async fn test_get_group_status() {
        let baglama = Baglama2::new().await.unwrap();
        let expected = Some(RowGroupStatus {
            id: 62776,
            group_id: 782,
            year: 2022,
            month: 10,
            status: "VIEW DATA COMPLETE".to_string(),
            total_views: Some(2062290),
            file: None,
            sqlite3: Some("/data/project/glamtools/viewdata/202210/782.sqlite3".to_string()),
        });
        let gs = baglama
            .get_group_status(&782.try_into().unwrap(), &YearMonth::new(2022, 10).unwrap())
            .await
            .unwrap();
        assert_eq!(gs, expected);
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
