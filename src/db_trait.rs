use crate::{
    global_image_links::GlobalImageLinks, Baglama2, GroupDate, Site, ViewCount, YearMonth,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

pub struct FilePart {
    pub site_id: usize,
    pub page_title: String,
    pub file: String,
}

impl FilePart {
    pub fn new(site_id: usize, title: String, gil_to: String) -> Self {
        Self {
            site_id,
            page_title: title,
            file: gil_to,
        }
    }
}

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
    async fn get_viewid_site_id_title(
        &self,
        parts: &[FilePart],
    ) -> Result<Vec<(usize, usize, String)>>;

    async fn add_views_batch_for_files(
        &self,
        sql_values: Vec<String>,
        parts: Vec<FilePart>,
        group_status_id: usize,
    ) -> Result<()> {
        if sql_values.is_empty() {
            return Ok(());
        }
        self.create_views_in_db(&parts, &sql_values).await?;
        let viewid_site_id_title = self.get_viewid_site_id_title(&parts).await?;

        let siteid_title_viewid: HashMap<(usize, String), usize> = viewid_site_id_title
            .into_iter()
            .map(|(view_id, site_id, title)| ((site_id, title.to_owned()), view_id))
            .collect();
        let mut values = vec![];
        let mut images = vec![];
        for part in &parts {
            let view_id = match siteid_title_viewid.get(&(part.site_id, part.page_title.to_owned()))
            {
                Some(id) => id,
                None => {
                    println!("{}/{} not found, odd", part.site_id, part.page_title);
                    continue;
                }
            };
            values.push(format!("({group_status_id},{view_id},?)"));
            images.push(part.file.to_owned());
        }
        if !values.is_empty() {
            self.insert_group2view(&values, images).await?;
        }
        Ok(())
    }

    async fn add_views_for_files(&self, all_files: &[String], gd: &GroupDate) -> Result<()> {
        if all_files.is_empty() {
            return Ok(());
        }

        let group_status_id = self.get_group_status_id().await?;

        const CHUNK_SIZE: usize = 3000;
        let mut chunk_num = 0;
        for files in all_files.chunks(CHUNK_SIZE) {
            chunk_num += 1;
            println!(
                "add_views_for_files: starting chunk {chunk_num} ({CHUNK_SIZE} of {} files total)",
                all_files.len()
            );
            let globalimagelinks = GlobalImageLinks::load(files, self.baglama2()).await?;
            println!("add_views_for_files: globalimagelinks done");
            let mut sql_values = vec![];
            let mut parts = vec![];
            for gil in &globalimagelinks {
                let site = match gd.get_site_for_wiki(&gil.wiki) {
                    Some(site) => site,
                    None => {
                        //println!("Unknown wiki: {}",&gil.wiki);
                        continue;
                    }
                };

                let site_id = site.id();
                let title = &gil.page_title;
                let month = self.ym().month();
                let year = self.ym().year();
                let done = 0;
                let namespace_id = gil.page_namespace_id;
                let page_id = gil.page;
                let views = 0;

                // site,title,month,year,done,namespace_id,page_id,views
                let sql_value =
                    format!("({site_id},?,{month},{year},{done},{namespace_id},{page_id},{views})");
                sql_values.push(sql_value);
                let part = FilePart::new(site_id, title.to_owned(), gil.to.to_owned());
                parts.push(part);
            }

            if !parts.is_empty() {
                self.add_views_batch_for_files(sql_values, parts, group_status_id)
                    .await?;
            }

            println!("add_views_for_files: batch done");
        }
        println!("add_views_for_files: all batches done");

        Ok(())
    }
}
