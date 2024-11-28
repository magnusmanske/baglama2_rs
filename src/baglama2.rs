use crate::row_group::RowGroup;
use crate::row_group_status::RowGroupStatus;
use crate::GroupId;
use crate::Site;
use crate::YearMonth;
use anyhow::Result;
use core::time::Duration;
use lazy_static::lazy_static;
use mysql_async::{from_row, prelude::*, Conn, Opts, OptsBuilder, PoolConstraints, PoolOpts};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0";
const URL_LOAD_TIMEOUT_SEC: u64 = 60;

lazy_static! {
    static ref RE_WEBSERVER_WIKIPEDIA: Regex = Regex::new(r"^(.+)wiki$").expect("Regex error");
    static ref RE_WEBSERVER_WIKI: Regex = Regex::new(r"^(.+)(wik.+)$").expect("Regex error");
}

#[derive(Debug)]
pub struct Baglama2 {
    config: Value,
    tool_db_pool: mysql_async::Pool,
    commons_pool: mysql_async::Pool,
    namespace_cache: Arc<Mutex<HashMap<String, HashMap<i32, String>>>>,
    sites_cache: Vec<Site>,
}

impl Baglama2 {
    pub async fn new() -> Result<Self> {
        let config = match Self::get_config_from_file("config.json") {
            Ok(config) => config,
            Err(_) => {
                Self::get_config_from_file("/data/project/glamtools/baglama2_rs/config.json")?
            }
        };
        let mut ret = Self {
            config: config.clone(),
            tool_db_pool: Self::create_pool(
                config.get("tooldb").expect("No conenction to tool DB"),
            )?,
            commons_pool: Self::create_pool(
                config.get("commons").expect("No conenction to Commons DB"),
            )?,
            namespace_cache: Arc::new(Mutex::new(HashMap::new())),
            sites_cache: vec![],
        };
        ret.populate_sites().await?;
        Ok(ret)
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

    fn create_pool(config: &Value) -> Result<mysql_async::Pool> {
        let min_connections = config["min_connections"].as_u64().unwrap_or(0) as usize;
        let max_connections = config["max_connections"].as_u64().unwrap_or(5) as usize;
        let keep_sec = config["keep_sec"].as_u64().unwrap_or(0);
        let url = config["url"].as_str().expect("No url value");
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(min_connections, max_connections).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(keep_sec));
        let opts = Opts::from_url(url)?;
        let opts = OptsBuilder::from_opts(opts).pool_opts(pool_opts);
        Ok(mysql_async::Pool::new(opts))
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

    pub async fn get_tooldb_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.tool_db_pool.get_conn().await
    }

    pub async fn get_commons_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.commons_pool.get_conn().await
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
        let sql = "SELECT id,FROM_BASE64(TO_BASE64(category)),depth,FROM_BASE64(TO_BASE64(added_by)),just_added FROM `groups` WHERE id=?";
        let groups = self
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, (group_id.as_usize(),))
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

    // TESTED
    pub fn fix_server_name_for_page_view_api(server: &str) -> String {
        match server {
            "wikidata.wikipedia.org" => "wikidata.org".to_string(),
            "species.wikipedia.org" => "species.wikimedia.org".to_string(),
            _ => server.to_string(),
        }
    }

    // TESTED
    pub fn get_webserver_for_wiki(wiki: &str) -> Option<String> {
        match wiki {
            "commonswiki" => Some("commons.wikimedia.org".to_string()),
            "wikidatawiki" => Some("www.wikidata.org".to_string()),
            "specieswiki" => Some("species.wikimedia.org".to_string()),
            "metawiki" => Some("meta.wikimedia.org".to_string()),
            wiki => {
                let wiki = wiki.replace("_", "-");
                if let Some(cap1) = RE_WEBSERVER_WIKIPEDIA.captures(&wiki) {
                    if let Some(name) = cap1.get(1) {
                        return Some(format!("{}.wikipedia.org", name.as_str()));
                    }
                }
                if let Some(cap2) = RE_WEBSERVER_WIKI.captures(&wiki) {
                    if let (Some(name), Some(domain)) = (cap2.get(1), cap2.get(2)) {
                        return Some(format!("{}.{}.org", name.as_str(), domain.as_str()));
                    }
                }
                None
            }
        }
    }

    pub fn reqwest_client_external() -> Option<reqwest::Client> {
        reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(core::time::Duration::from_secs(URL_LOAD_TIMEOUT_SEC))
            .connection_verbose(true)
            .gzip(true)
            .deflate(true)
            .brotli(true)
            .build()
            .ok()
    }

    pub async fn load_json_from_url(url: &str) -> Option<Value> {
        let json_text = Self::reqwest_client_external()?
            .get(url)
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        let json: Value = serde_json::from_str(&json_text).ok()?;
        Some(json)
    }

    // TESTED
    pub async fn load_namespaces_for_wiki(&self, wiki: &str) -> Result<bool> {
        if self.namespace_cache.lock().await.contains_key(wiki) {
            return Ok(true);
        }
        let server = match Self::get_webserver_for_wiki(wiki) {
            Some(server) => server,
            None => return Ok(false),
        };
        let url = format!(
            "https://{server}/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json"
        );
        let json = match Self::load_json_from_url(&url).await {
            Some(json) => json,
            None => return Ok(false),
        };

        let mut m = HashMap::new();
        if let Some(namespaces) = json["query"]["namespaces"].as_object() {
            for (namespace_id, data) in namespaces {
                if let Ok(namespace_id) = namespace_id.parse::<i32>() {
                    if let Some(canonical_name) = data["*"].as_str() {
                        m.insert(namespace_id, canonical_name.to_string());
                    }
                };
            }
        }

        let mut namespace_cache = self.namespace_cache.lock().await;
        if !namespace_cache.contains_key(wiki) {
            namespace_cache.insert(wiki.to_owned(), m);
        }

        Ok(true)
    }

    // TESTED
    pub async fn prefix_with_namespace(
        &self,
        title: &str,
        namespace_id: i32,
        wiki: &str,
    ) -> Option<String> {
        if !self.load_namespaces_for_wiki(wiki).await.ok()? {
            return None;
        }
        let prefix = self
            .namespace_cache
            .lock()
            .await
            .get(wiki)?
            .get(&namespace_id)?
            .to_owned();
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
                        return Err(e.into());
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

    // TESTED
    pub async fn get_next_group_id(&self, year: i32, month: u32) -> Option<usize> {
        let mut conn = self.get_tooldb_conn().await.ok()?;
        let sql = "SELECT id FROM groups WHERE is_active=1 AND NOT EXISTS (SELECT * FROM group_status WHERE groups.id=group_id AND year=:year AND month=:month) ORDER BY rand() LIMIT 1";
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
    async fn test_load_namespaces_for_wiki() {
        let baglama = Baglama2::new().await.unwrap();
        assert!(baglama.load_namespaces_for_wiki("enwiki").await.unwrap());
        assert_eq!(baglama.namespace_cache.lock().await["enwiki"][&1], "Talk");
    }

    #[tokio::test]
    async fn test_get_next_group_id() {
        let baglama = Baglama2::new().await.unwrap();
        let ng = baglama.get_next_group_id(2014, 1).await;
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
    async fn test_get_webserver_for_wiki() {
        assert_eq!(
            Baglama2::get_webserver_for_wiki("wikidatawiki"),
            Some("www.wikidata.org".to_string())
        );
        assert_eq!(
            Baglama2::get_webserver_for_wiki("enwiki"),
            Some("en.wikipedia.org".to_string())
        );
        assert_eq!(
            Baglama2::get_webserver_for_wiki("enwikisource"),
            Some("en.wikisource.org".to_string())
        );
        assert_eq!(Baglama2::get_webserver_for_wiki("shcswirk8d7g"), None);
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

    #[test]
    fn test_fix_server_name_for_page_view_api() {
        assert_eq!(
            Baglama2::fix_server_name_for_page_view_api("en.wikipedia.org"),
            "en.wikipedia.org"
        );
        assert_eq!(
            Baglama2::fix_server_name_for_page_view_api("species.wikipedia.org"),
            "species.wikimedia.org"
        );
        assert_eq!(
            Baglama2::fix_server_name_for_page_view_api("wikidata.wikipedia.org"),
            "wikidata.org"
        );
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
