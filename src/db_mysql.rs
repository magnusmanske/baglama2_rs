use crate::{Baglama2, GroupDate, GroupId, Site, ViewCount, YearMonth};
use anyhow::Result;
use std::sync::Arc;

// WORK IN PROGRESS, DO NOT USE YET

#[derive(Debug, Clone)]
pub struct DbMySql {
    baglama: Arc<Baglama2>,
    _ym: YearMonth,
    _group_id: GroupId,
}

impl DbMySql {
    pub fn new(gd: &GroupDate, baglama: Arc<Baglama2>) -> Result<Self> {
        Ok(Self {
            baglama,
            _ym: gd.ym().to_owned(),
            _group_id: gd.group_id(),
        })
    }

    pub fn path_final(&self) -> &str {
        "" // Not needed for MySQL
    }

    pub fn finalize(&self) -> Result<()> {
        todo!()
    }

    pub fn load_sites(&self) -> Result<Vec<Site>> {
        self.baglama.get_sites()
    }

    pub fn get_view_counts(&self, batch_size: usize) -> Result<Vec<ViewCount>> {
        let _sql = format!("SELECT DISTINCT `views`.`id` AS id,title,namespace_id,grok_code,server,done,`views`.`site` AS site_id FROM `views`,`sites` WHERE `done`=0 AND `sites`.`id`=`views`.`site` LIMIT {batch_size}");
        todo!()
    }

    pub fn get_group_status_id(&self) -> Result<usize> {
        // let group_id = self.group_id.to_owned();
        // let year = self.ym.year();
        // let month = self.ym.month();
        // let sql = "SELECT `id` FROM `group_status` WHERE `group_id`=? AND `year`=? AND `month`=?";
        todo!()
    }

    pub fn get_total_views(&self, _group_status_id: usize) -> Result<usize> {
        let _sql = "SELECT ifnull(total_views,0) FROM group_status WHERE id=?";
        todo!()
    }

    pub fn create_final_indices(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    pub fn delete_all_files(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    pub fn delete_views(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    pub fn delete_group2view(&self) -> Result<()> {
        // Not needed for MySQL
        Ok(())
    }

    pub fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>> {
        let _sql = format!("SELECT `filename` FROM `files` LIMIT {batch_size} OFFSET {offset}");
        todo!()
    }

    pub async fn add_views_for_files(&self, _all_files: &[String], _gd: &GroupDate) -> Result<()> {
        // BIG ONE
        todo!()
    }

    pub fn reset_main_page_view_count(&self) -> Result<()> {
        // TODO for all wikis?
        let _sql = "UPDATE views SET views=0 WHERE title='Main_Page'"; // FIXME
        todo!()
    }

    pub async fn add_summary_statistics(&self, _group_status_id: usize) -> Result<()> {
        todo!()
    }

    pub async fn update_view_count(&self, view_id: usize, view_count: u64) -> Result<usize> {
        let _sql = format!("UPDATE `views` SET `done`=1,`views`={view_count} WHERE `id`={view_id}"); // FIXME
        todo!()
    }

    pub async fn view_done(&self, view_id: usize, done: u8) -> Result<usize> {
        let _sql = format!("UPDATE `views` SET `done`={done},`views`=0 WHERE `id`={view_id}");
        todo!()
    }

    pub fn file_insert_batch_size(&self) -> usize {
        5000 // Dunno?
    }

    pub fn insert_files_batch(&self, batch: &[String]) -> Result<()> {
        let questionmarks = ["(?)"].repeat(batch.len()).join(",");
        let _sql = format!("INSERT INTO `files` (`filename`) VALUES {questionmarks}");
        todo!()
    }

    pub async fn initialize(&self) -> Result<()> {
        todo!()
    }
}
