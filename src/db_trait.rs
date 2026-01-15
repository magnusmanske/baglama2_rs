use crate::{Baglama2, DbId, Site, ViewCount, YearMonth};
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::prelude::FromRow;
use std::sync::Arc;

#[derive(Debug)]
pub struct FilePart {
    pub id: Option<DbId>,
    pub site_id: DbId,
    pub page_title: String,
    pub page_id: DbId,
    pub file: String,
}

impl FilePart {
    pub fn new(site_id: DbId, page_title: String, page_id: DbId, file: String) -> Self {
        Self {
            id: None,
            site_id,
            page_title,
            page_id,
            file,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ViewIdSiteIdTitle {
    pub view_id: DbId,
    pub site_id: DbId,
    pub title: String,
}

impl ViewIdSiteIdTitle {
    pub fn new(view_id: DbId, site_id: DbId, title: String) -> Self {
        Self {
            view_id,
            site_id,
            title,
        }
    }

    pub fn from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        let view_id: isize = row.get(0)?;
        let site_id: isize = row.get(1)?;
        Ok(Self {
            view_id: view_id as DbId,
            site_id: site_id as DbId,
            title: row.get(2)?,
        })
    }
}

impl FromRow for ViewIdSiteIdTitle {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let title: Vec<u8> = row
            .get(2)
            .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?;
        let title =
            String::from_utf8(title).map_err(|_| mysql_async::FromRowError(row.to_owned()))?;
        Ok(Self::new(
            row.get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            row.get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            title,
        ))
    }
}

#[async_trait]
pub trait DbTrait {
    fn path_final(&self) -> &str;
    async fn finalize(&self) -> Result<()>;
    fn load_sites(&self) -> Result<Vec<Site>>;
    async fn get_view_counts_todo(&self, batch_size: usize) -> Result<Vec<ViewCount>>;
    async fn get_group_status_id(&self) -> Result<DbId>;
    async fn get_total_views(&self, group_status_id: DbId) -> Result<isize>;
    fn create_final_indices(&self) -> Result<()>;
    async fn delete_all_files(&self) -> Result<()>;
    fn delete_views(&self) -> Result<()>;
    fn delete_group2view(&self) -> Result<()>;
    async fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>>;
    async fn reset_main_page_view_count(&self) -> Result<()>;
    async fn add_summary_statistics(&self, group_status_id: DbId) -> Result<()>;
    async fn update_view_count(&self, view_id: DbId, view_count: i64) -> Result<()>;
    async fn view_done(&self, view_id: DbId, done: u8) -> Result<()>;
    fn file_insert_batch_size(&self) -> isize;
    async fn insert_files_batch(&self, batch: &[String]) -> Result<()>;
    async fn initialize(&self) -> Result<()>;
    fn baglama2(&self) -> &Arc<Baglama2>;
    fn ym(&self) -> &YearMonth;
    async fn create_views_in_db(&self, parts: &[FilePart], sql_values: &[String]) -> Result<()>;
    async fn insert_group2view(&self, values: &[String], images: Vec<String>) -> Result<()>;
    async fn get_viewid_site_id_title(&self, parts: &[FilePart]) -> Result<Vec<ViewIdSiteIdTitle>>;
}
