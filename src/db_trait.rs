use crate::{Baglama2, Site, ViewCount, YearMonth};
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::prelude::FromRow;
use std::sync::Arc;

#[derive(Debug)]
pub struct FilePart {
    pub site_id: usize,
    pub page_title: String,
    pub page_id: usize,
    pub file: String,
}

impl FilePart {
    pub fn new(site_id: usize, page_title: String, page_id: usize, file: String) -> Self {
        Self {
            site_id,
            page_title,
            page_id,
            file,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ViewIdSiteIdTitle {
    pub view_id: usize,
    pub site_id: usize,
    pub title: String,
}

impl ViewIdSiteIdTitle {
    pub fn new(view_id: usize, site_id: usize, title: String) -> Self {
        Self {
            view_id,
            site_id,
            title,
        }
    }

    pub fn from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            view_id: row.get(0)?,
            site_id: row.get(1)?,
            title: row.get(2)?,
        })
    }
}

impl FromRow for ViewIdSiteIdTitle {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self::new(
            row.get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            row.get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            row.get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        ))
    }
}

#[async_trait]
pub trait DbTrait {
    fn path_final(&self) -> &str;
    async fn finalize(&self) -> Result<()>;
    fn load_sites(&self) -> Result<Vec<Site>>;
    async fn get_view_counts_todo(&self, batch_size: usize) -> Result<Vec<ViewCount>>;
    async fn get_group_status_id(&self) -> Result<usize>;
    async fn get_total_views(&self, group_status_id: usize) -> Result<usize>;
    fn create_final_indices(&self) -> Result<()>;
    async fn delete_all_files(&self) -> Result<()>;
    fn delete_views(&self) -> Result<()>;
    fn delete_group2view(&self) -> Result<()>;
    async fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>>;
    async fn reset_main_page_view_count(&self) -> Result<()>;
    async fn add_summary_statistics(&self, group_status_id: usize) -> Result<()>;
    async fn update_view_count(&self, view_id: usize, view_count: u64) -> Result<()>;
    async fn view_done(&self, view_id: usize, done: u8) -> Result<()>;
    fn file_insert_batch_size(&self) -> usize;
    async fn insert_files_batch(&self, batch: &[String]) -> Result<()>;
    async fn initialize(&self) -> Result<()>;
    fn baglama2(&self) -> &Arc<Baglama2>;
    fn ym(&self) -> &YearMonth;
    async fn create_views_in_db(&self, parts: &[FilePart], sql_values: &[String]) -> Result<()>;
    async fn insert_group2view(&self, values: &[String], images: Vec<String>) -> Result<()>;
    async fn get_viewid_site_id_title(&self, parts: &[FilePart]) -> Result<Vec<ViewIdSiteIdTitle>>;
}
