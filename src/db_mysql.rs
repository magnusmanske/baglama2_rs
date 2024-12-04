use crate::{db_trait::DbTrait, Baglama2, GroupDate, GroupId, Site, ViewCount, YearMonth};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::{from_row, prelude::*};
use std::sync::Arc;
use tokio::sync::Mutex;

// WORK IN PROGRESS, DO NOT USE YET

#[derive(Debug, Clone)]
pub struct DbMySql {
    baglama: Arc<Baglama2>,
    ym: YearMonth,
    group_id: GroupId,
    files: Arc<Mutex<Vec<String>>>,
}

impl DbMySql {
    pub fn new(gd: &GroupDate, baglama: Arc<Baglama2>) -> Result<Self> {
        Ok(Self {
            baglama,
            ym: gd.ym().to_owned(),
            group_id: gd.group_id(),
            files: Arc::new(Mutex::new(Vec::new())),
        })
    }

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

    async fn execute(&self, sql: &str) -> Result<()> {
        self.baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl DbTrait for DbMySql {
    fn path_final(&self) -> &str {
        "" // Not needed for MySQL
    }

    fn finalize(&self) -> Result<()> {
        todo!()
    }

    fn load_sites(&self) -> Result<Vec<Site>> {
        self.baglama.get_sites()
    }

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

    async fn get_group_status_id(&self) -> Result<usize> {
        let group_id = self.group_id.to_owned();
        let year = self.ym.year();
        let month = self.ym.month();
        let sql = "SELECT `id` FROM `group_status` WHERE `group_id`=? AND `year`=? AND `month`=?";
        let ret = self
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
        Ok(ret)
    }

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

    async fn delete_all_files(&self) -> Result<()> {
        self.files.lock().await.clear();
        Ok(())
    }

    fn delete_views(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    fn delete_group2view(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    async fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>> {
        let files = self.files.lock().await;
        let end = (offset + batch_size).min(files.len());
        let ret = files[offset..end].to_vec();
        Ok(ret)
    }

    async fn add_views_for_files(&self, _all_files: &[String], _gd: &GroupDate) -> Result<()> {
        // BIG ONE
        todo!()
    }

    async fn reset_main_page_view_count(&self) -> Result<()> {
        // TODO for all wikis?
        let group_status_id = self.get_group_status_id().await?;
        let sql = "SELECT `view_id` FROM `group2view` WHERE `group_status_id`=? AND `view_id`=`views`.`id`";
        let sql =
            format!("UPDATE views SET views=0 WHERE title='Main_Page' AND views.id IN ({sql})");
        self.baglama
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, (group_status_id,))
            .await?;
        Ok(())
    }

    async fn add_summary_statistics(&self, group_status_id: usize) -> Result<()> {
        self.update_gs2site(group_status_id).await?;
        self.update_group_status(group_status_id).await?;
        Ok(())
    }

    async fn update_view_count(&self, view_id: usize, view_count: u64) -> Result<()> {
        let sql = format!("UPDATE `views` SET `done`=1,`views`={view_count} WHERE `id`={view_id}");
        self.execute(&sql).await
    }

    async fn view_done(&self, view_id: usize, done: u8) -> Result<()> {
        let sql = format!("UPDATE `views` SET `done`={done},`views`=0 WHERE `id`={view_id}");
        self.execute(&sql).await
    }

    fn file_insert_batch_size(&self) -> usize {
        5000 // Dunno?
    }

    async fn insert_files_batch(&self, batch: &[String]) -> Result<()> {
        self.files.lock().await.extend_from_slice(batch);
        Ok(())
    }

    async fn initialize(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_group_status_id() {
        let baglama = Arc::new(Baglama2::new().await.unwrap());
        let group_id = GroupId::try_from(1).unwrap();
        let ym = YearMonth::new(2014, 2).unwrap();
        let db = DbMySql::new(&GroupDate::new(group_id, ym, baglama.clone()), baglama).unwrap();
        let ret = db.get_group_status_id().await.unwrap();
        assert_eq!(ret, 1);
    }

    #[tokio::test]
    async fn test_get_view_counts_todo() {
        let baglama = Arc::new(Baglama2::new().await.unwrap());
        let group_id = GroupId::try_from(1).unwrap();
        let ym = YearMonth::new(2014, 2).unwrap();
        let db = DbMySql::new(&GroupDate::new(group_id, ym, baglama.clone()), baglama).unwrap();
        let ret = db.get_view_counts_todo(10).await.unwrap();
        // This might depend on the MySQL sort order
        assert_eq!(ret[0].site_id, 69);
        assert_eq!(ret[9].site_id, 417);
    }

    #[tokio::test]
    async fn test_get_total_views() {
        let baglama = Arc::new(Baglama2::new().await.unwrap());
        let group_id = GroupId::try_from(1).unwrap();
        let ym = YearMonth::new(2014, 2).unwrap();
        let db = DbMySql::new(&GroupDate::new(group_id, ym, baglama.clone()), baglama).unwrap();
        let group_status_id = db.get_group_status_id().await.unwrap();
        assert_eq!(group_status_id, 1);
        let ret = db.get_total_views(group_status_id).await.unwrap();
        assert_eq!(ret, 722790216);
    }
}
