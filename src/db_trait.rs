use crate::{GroupDate, Site, ViewCount};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait DbTrait {
    fn path_final(&self) -> &str;
    fn finalize(&self) -> Result<()>;
    fn load_sites(&self) -> Result<Vec<Site>>;
    async fn get_view_counts_todo(&self, batch_size: usize) -> Result<Vec<ViewCount>>;
    async fn get_group_status_id(&self) -> Result<usize>;
    async fn get_total_views(&self, group_status_id: usize) -> Result<usize>;
    fn create_final_indices(&self) -> Result<()>;
    async fn delete_all_files(&self) -> Result<()>;
    fn delete_views(&self) -> Result<()>;
    fn delete_group2view(&self) -> Result<()>;
    async fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>>;
    async fn add_views_for_files(&self, _all_files: &[String], _gd: &GroupDate) -> Result<()>;
    async fn reset_main_page_view_count(&self) -> Result<()>;
    async fn add_summary_statistics(&self, group_status_id: usize) -> Result<()>;
    async fn update_view_count(&self, view_id: usize, view_count: u64) -> Result<()>;
    async fn view_done(&self, view_id: usize, done: u8) -> Result<()>;
    fn file_insert_batch_size(&self) -> usize;
    async fn insert_files_batch(&self, batch: &[String]) -> Result<()>;
    async fn initialize(&self) -> Result<()>;
}
