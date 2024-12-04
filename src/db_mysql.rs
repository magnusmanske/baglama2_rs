use crate::{
    db_trait::{DbTrait, FilePart, ViewIdSiteIdTitle},
    Baglama2, GroupDate, GroupId, Site, ViewCount, YearMonth,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::{from_row, prelude::*};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

// WORK IN PROGRESS, DO NOT USE YET

#[derive(Debug, Clone)]
pub struct DbMySql {
    baglama: Arc<Baglama2>,
    ym: YearMonth,
    group_id: GroupId,
    group_status_id: OnceCell<usize>,
    testing: bool,
    test_log: Arc<Mutex<Vec<Value>>>,
}

impl DbMySql {
    pub fn new(gd: &GroupDate, baglama: Arc<Baglama2>) -> Result<Self> {
        Ok(Self {
            baglama,
            ym: gd.ym().to_owned(),
            group_id: gd.group_id(),
            group_status_id: OnceCell::new(),
            testing: false,
            test_log: Arc::new(Mutex::new(vec![])),
        })
    }

    /// Used for internal testing only
    fn _as_test(self) -> Self {
        Self {
            testing: true,
            ..self
        }
    }

    // tested
    /// Sets the group_status to 'VIEW DATA COMPLETE' and updates the `total_views` field
    async fn update_group_status(&self, group_status_id: usize) -> Result<()> {
        let sql = format!(
                "UPDATE group_status
                	SET status='VIEW DATA COMPLETE',
                 	total_views=(SELECT sum(views) FROM gs2site WHERE `group_status_id`={group_status_id})
                  	WHERE id={group_status_id}"
            );
        self.execute(&sql).await?;
        Ok(())
    }

    // tested
    async fn update_gs2site(&self, group_status_id: usize) -> Result<()> {
        let sql = format!("DELETE FROM `gs2site` WHERE `group_status_id`={group_status_id}");
        self.execute(&sql).await?;
        let sql = format!(
                "INSERT INTO `gs2site` (group_status_id,site_id,pages,views)
                	SELECT {group_status_id},sites.id,COUNT(DISTINCT page_id),SUM(views)
                 	FROM `views`,`sites`,`group2view`
                  	WHERE views.site=sites.id AND view_id=views.id AND group_status_id={group_status_id}
                   	GROUP BY sites.id"
            );
        self.execute(&sql).await?;
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

    /// Runs a single SQL query with no parameters, and no return value
    /// Does not run in testing mode
    async fn execute(&self, sql: &str) -> Result<()> {
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
impl DbTrait for DbMySql {
    fn path_final(&self) -> &str {
        "" // Not needed for MySQL
    }

    fn baglama2(&self) -> &Arc<Baglama2> {
        &self.baglama
    }

    fn ym(&self) -> &YearMonth {
        &self.ym
    }

    fn finalize(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    fn load_sites(&self) -> Result<Vec<Site>> {
        self.baglama.get_sites()
    }

    // tested
    async fn get_view_counts_todo(&self, batch_size: usize) -> Result<Vec<ViewCount>> {
        let group_status_id = self.get_group_status_id().await?;
        let sql = format!("SELECT DISTINCT `views`.`id` AS id,title,namespace_id,grok_code,server,done,`views`.`site` AS site_id
        	FROM `views`,`sites`,`group2view`
         	WHERE `done`=0 AND `sites`.`id`=`views`.`site` AND `group_status_id`={group_status_id} AND `view_id`=`views`.`id`
          	LIMIT {batch_size}");
        let ret = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<ViewCount>)
            .await?;
        Ok(ret)
    }

    // tested
    async fn get_group_status_id(&self) -> Result<usize> {
        if let Some(ret) = self.group_status_id.get() {
            return Ok(*ret);
        }
        let group_id = self.group_id.to_owned();
        let year = self.ym.year();
        let month = self.ym.month();
        let sql = "SELECT `id` FROM `group_status` WHERE `group_id`=? AND `year`=? AND `month`=?";
        let group_status_id = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, (group_id.as_usize(), year, month))
            .await?
            .map_and_drop(from_row::<usize>)
            .await?
            .first()
            .ok_or_else(|| anyhow!("No group_status.id for group {group_id} in {year}-{month}"))?
            .to_owned();
        let ret = self
            .group_status_id
            .get_or_init(|| async { group_status_id })
            .await;
        Ok(*ret)
    }

    // tested
    async fn get_total_views(&self, group_status_id: usize) -> Result<usize> {
        let sql = "SELECT ifnull(total_views,0) FROM group_status WHERE id=?";
        let ret = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, (group_status_id,))
            .await?
            .map_and_drop(from_row::<usize>)
            .await?
            .first()
            .ok_or_else(|| anyhow!("get_total_views for group_status_id {group_status_id}"))?
            .to_owned();
        Ok(ret)
    }

    fn create_final_indices(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    // tested
    async fn delete_all_files(&self) -> Result<()> {
        let group_status_id = self.get_group_status_id().await?;
        let sql = format!("DELETE FROM `files` WHERE `group_status_id`={group_status_id}");
        self.execute(&sql).await
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
    async fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>> {
        let group_status_id = self.get_group_status_id().await?;
        let sql = format!("SELECT `name` FROM `files` WHERE `group_status_id`={group_status_id} LIMIT {batch_size} OFFSET {offset}");
        let ret = self
            .baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(ret)
    }

    // tested
    async fn reset_main_page_view_count(&self) -> Result<()> {
        // TODO for all wikis?
        let group_status_id = self.get_group_status_id().await?;
        let sql = format!("SELECT `view_id` FROM `group2view` WHERE `group_status_id`={group_status_id} AND `view_id`=`views`.`id`");
        let sql =
            format!("UPDATE views SET views=0 WHERE title='Main_Page' AND views.id IN ({sql})");
        self.execute(&sql).await
    }

    // components are tested
    async fn add_summary_statistics(&self, group_status_id: usize) -> Result<()> {
        self.update_gs2site(group_status_id).await?;
        self.update_group_status(group_status_id).await?;
        Ok(())
    }

    // tested
    async fn update_view_count(&self, view_id: usize, view_count: u64) -> Result<()> {
        let sql = format!("UPDATE `views` SET `done`=1,`views`={view_count} WHERE `id`={view_id}");
        self.execute(&sql).await
    }

    // tested
    /// Mark a view as done and set the view count to 0; usually for failures
    async fn view_done(&self, view_id: usize, done: u8) -> Result<()> {
        let sql = format!("UPDATE `views` SET `done`={done},`views`=0 WHERE `id`={view_id}");
        self.execute(&sql).await
    }

    fn file_insert_batch_size(&self) -> usize {
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
        let sql =
            format!("INSERT IGNORE INTO `files` (`group_status_id`,`name`) VALUES {placeholders}");
        self.exec_vec(&sql, batch.to_vec()).await?;
        Ok(())
    }

    // components tested
    async fn initialize(&self) -> Result<()> {
        self.baglama2()
            .set_group_status(self.group_id.to_owned(), &self.ym, "", 0, "")
            .await?;
        self.delete_all_files().await?;
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
            .map_and_drop(from_row::<ViewIdSiteIdTitle>)
            .await?;
        Ok(viewid_site_id_title)
    }

    // tested
    async fn create_views_in_db(&self, parts: &[FilePart], sql_values: &[String]) -> Result<()> {
        let sql = "INSERT OR IGNORE INTO `views` (site,title,month,year,done,namespace_id,page_id,views) VALUES ".to_string() + &sql_values.join(",");
        let titles: Vec<String> = parts.iter().map(|p| p.page_title.to_owned()).collect();
        self.exec_vec(&sql, titles).await?;
        Ok(())
    }

    // tested
    async fn insert_group2view(&self, values: &[String], images: Vec<String>) -> Result<()> {
        let sql = "INSERT OR IGNORE INTO `group2view` (group_status_id,view_id,image) VALUES "
            .to_string()
            + &values.join(",").to_string();
        self.exec_vec(&sql, images).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a new test DB
    async fn new_test_db(group_id: usize, year: i32, month: u32) -> Result<DbMySql> {
        let baglama = Arc::new(Baglama2::new().await.unwrap());
        let group_id = GroupId::try_from(group_id).unwrap();
        let ym = YearMonth::new(year, month).unwrap();
        let db = DbMySql::new(&GroupDate::new(group_id, ym, baglama.clone()), baglama).unwrap();
        Ok(db)
    }

    #[tokio::test]
    async fn test_get_group_status_id() {
        let db = new_test_db(1, 2014, 2).await.unwrap();
        let ret = db.get_group_status_id().await.unwrap();
        assert_eq!(ret, 1);
    }

    #[tokio::test]
    async fn test_get_view_counts_todo() {
        let db = new_test_db(1, 2014, 2).await.unwrap();
        let ret = db.get_view_counts_todo(10).await.unwrap();
        // This might depend on the MySQL sort order
        assert_eq!(ret[0].site_id, 69);
        assert_eq!(ret[9].site_id, 417);
    }

    #[tokio::test]
    async fn test_get_total_views() {
        let db = new_test_db(1, 2014, 2).await.unwrap();
        let group_status_id = db.get_group_status_id().await.unwrap();
        assert_eq!(group_status_id, 1);
        let ret = db.get_total_views(group_status_id).await.unwrap();
        assert_eq!(ret, 722790216);
    }

    #[tokio::test]
    async fn test_insert_group2view() {
        let db = new_test_db(1, 2014, 2).await.unwrap()._as_test();
        let values = vec!["(1,2,?)".to_string()];
        let images = vec!["bar".to_string()];
        db.insert_group2view(&values, images).await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
            *db.test_log.lock().await,
            [
                json!({"payload": ["bar"], "sql": "INSERT OR IGNORE INTO `group2view` (group_status_id,view_id,image) VALUES (1,2,?)"})
            ]
        );
    }

    #[tokio::test]
    async fn test_create_views_in_db() {
        let db = new_test_db(1, 2014, 2).await.unwrap()._as_test();
        let parts = vec![FilePart::new(
            1,
            "The_Page_Title".to_string(),
            0,
            "The_File.jpg".to_string(),
        )];
        let sql_values = vec!["(12,?,3,2021,0,7,12345,67890)".to_string()];
        db.create_views_in_db(&parts, &sql_values).await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
            *db.test_log.lock().await,
            [
                json!({"payload":["The_Page_Title"],"sql":"INSERT OR IGNORE INTO `views` (site,title,month,year,done,namespace_id,page_id,views) VALUES (12,?,3,2021,0,7,12345,67890)"})
            ]
        );
    }

    #[tokio::test]
    async fn test_update_gs2site() {
        let db = new_test_db(1, 2014, 2).await.unwrap()._as_test();
        db.update_gs2site(123).await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
                *db.test_log.lock().await,
                [
                "DELETE FROM `gs2site` WHERE `group_status_id`=123",
                "INSERT INTO `gs2site` (group_status_id,site_id,pages,views) SELECT 123,sites.id,COUNT(DISTINCT page_id),SUM(views) FROM `views`,`sites`,`group2view` WHERE views.site=sites.id AND view_id=views.id AND group_status_id=123 GROUP BY sites.id"
                ]
            );
    }

    #[tokio::test]
    async fn test_delete_all_files() {
        let db = new_test_db(15, 2014, 2).await.unwrap()._as_test();
        db.delete_all_files().await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
            *db.test_log.lock().await,
            ["DELETE FROM `files` WHERE `group_status_id`=29"]
        );
    }

    #[tokio::test]
    async fn test_update_view_count() {
        let db = new_test_db(15, 2014, 2).await.unwrap()._as_test();
        db.update_view_count(12345, 67890).await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
            *db.test_log.lock().await,
            ["UPDATE `views` SET `done`=1,`views`=67890 WHERE `id`=12345"]
        );
    }

    #[tokio::test]
    async fn test_view_done() {
        let db = new_test_db(15, 2014, 2).await.unwrap()._as_test();
        db.view_done(12345, 1).await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
            *db.test_log.lock().await,
            ["UPDATE `views` SET `done`=1,`views`=0 WHERE `id`=12345"]
        );
    }

    #[tokio::test]
    async fn test_update_group_status() {
        let db = new_test_db(15, 2014, 2).await.unwrap()._as_test();
        db.update_group_status(12345).await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
                *db.test_log.lock().await,
                ["UPDATE group_status SET status='VIEW DATA COMPLETE', total_views=(SELECT sum(views) FROM gs2site WHERE `group_status_id`=12345) WHERE id=12345"]
            );
    }

    #[tokio::test]
    async fn test_reset_main_page_view_count() {
        let db = new_test_db(15, 2014, 2).await.unwrap()._as_test();
        db.reset_main_page_view_count().await.unwrap();
        // println!("{}", json!(*db.test_log.lock().await));
        assert_eq!(
            *db.test_log.lock().await,
            ["UPDATE views SET views=0 WHERE title='Main_Page' AND views.id IN (SELECT `view_id` FROM `group2view` WHERE `group_status_id`=29 AND `view_id`=`views`.`id`)"]
        );
    }

    #[tokio::test]
    async fn test_insert_files_batch() {
        let db = new_test_db(15, 2014, 2).await.unwrap();
        db.group_status_id.get_or_init(|| async { 0 }).await;
        assert_eq!(*db.group_status_id.get().unwrap(), 0);
        db.delete_all_files().await.unwrap(); // Clear the slate
        db.insert_files_batch(&["foo".to_string(), "bar".to_string()])
            .await
            .unwrap();
        assert_eq!(db.load_files_batch(0, 1).await.unwrap(), ["foo"]);
        assert_eq!(db.load_files_batch(1, 1).await.unwrap(), ["bar"]);
        db.delete_all_files().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_viewid_site_id_title() {
        let db = new_test_db(1, 2014, 2).await.unwrap()._as_test();
        let parts = vec![
            FilePart::new(158, "MeekMark".to_string(), 5153256, "Foo.jpg".to_string()),
            FilePart::new(165, "typesetter".to_string(), 224872, "Bar.jpg".to_string()),
        ];
        let result = db.get_viewid_site_id_title(&parts).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            ViewIdSiteIdTitle::new(4, 158, "MeekMark".to_string())
        );
        assert_eq!(
            result[1],
            ViewIdSiteIdTitle::new(5, 165, "typesetter".to_string())
        );
    }
}
