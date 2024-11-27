use crate::baglama2::*;
use crate::db_sqlite::DbSqlite;
use crate::GroupId;
use crate::Site;
use crate::ViewCount;
use crate::YearMonth;
use anyhow::Result;
use futures::future::join_all;
use mysql_async::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

const ADD_VIEW_COUNTS_BATCH_SIZE: usize = 3000;

#[derive(Debug, Clone)]
pub struct GroupDate {
    group_id: GroupId,
    ym: YearMonth,
    sites: HashMap<usize, Site>,
    wiki2site_id: HashMap<String, usize>,
    baglama: Arc<Baglama2>,
}

impl GroupDate {
    pub fn new(id: GroupId, ym: YearMonth, baglama: Arc<Baglama2>) -> Self {
        Self {
            group_id: id,
            ym,
            sites: HashMap::new(),
            wiki2site_id: HashMap::new(),
            baglama,
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn ym(&self) -> &YearMonth {
        &self.ym
    }

    async fn load_sites(&mut self, db: &DbSqlite) -> Result<()> {
        for site in db.load_sites()? {
            if let Some(wiki) = self.site2wiki(&site) {
                self.wiki2site_id.insert(wiki, site.id());
                self.sites.insert(site.id(), site);
            }
        }
        Ok(())
    }

    fn get_view_counts(&self, db: &DbSqlite, batch_size: usize) -> Result<Vec<ViewCount>> {
        let ret = db.get_view_counts(batch_size)?;
        Ok(ret)
    }

    pub fn get_site_for_wiki(&self, wiki: &str) -> Option<&Site> {
        match self.wiki2site_id.get(wiki) {
            Some(site_id) => self.sites.get(site_id),
            None => None,
        }
    }

    async fn get_files_from_commons_category_tree(
        &self,
        category: &str,
        depth: usize,
    ) -> Result<Vec<String>> {
        self.baglama.get_pages_in_category(category, depth, 6).await
    }

    pub async fn add_files_to_sqlite(&self, db: &DbSqlite) -> Result<()> {
        let (category, depth) = db.get_category_and_depth(self.group_id())?;
        db.delete_all_files()?;

        // Get files in category tree from Commons
        let files = self
            .get_files_from_commons_category_tree(&category, depth)
            .await?;

        let batch_size = db.file_insert_batch_size();
        for batch in files.chunks(batch_size) {
            db.insert_files_batch(batch)?;
        }

        Ok(())
    }

    pub async fn add_pages_to_sqlite(&mut self, db: &DbSqlite) -> Result<()> {
        self.load_sites(db).await?;
        db.delete_views()?;
        db.delete_group2view()?;
        let mut offset: usize = 0;
        const BATCH_SIZE: usize = 10000;
        loop {
            let files = db.load_files_batch(offset, BATCH_SIZE)?;
            let _ = db.add_views_for_files(&files, self).await;
            offset += files.len();
            if files.len() != BATCH_SIZE {
                break;
            }
        }
        Ok(())
    }

    // TESTED
    pub async fn get_total_monthly_page_views(
        &self,
        server: &str,
        title: &str,
        first_day: &str,
        last_day: &str,
    ) -> Option<u64> {
        let server = Baglama2::fix_server_name_for_page_view_api(server);
        let url = format!("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{server}/all-access/user/{title}/daily/{first_day}/{last_day}");
        let json_text = Baglama2::reqwest_client_external()?
            .get(url)
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        let json: Value = serde_json::from_str(&json_text).ok()?;
        let items = json.get("items")?.as_array()?;
        Some(
            items
                .iter()
                .filter_map(|item| item.get("views"))
                .filter_map(|views| views.as_u64())
                .sum(),
        )
    }

    fn site2wiki(&self, site: &Site) -> Option<String> {
        let (language, project) = match (site.language(), site.project()) {
            (Some(l), Some(p)) => (l, p),
            _ => return None,
        };
        if language == "commons" {
            Some("commonswiki".to_string())
        } else if project == "wikipedia" {
            Some(format!("{}wiki", language))
        } else {
            Some(format!("{}{}", language, project))
        }
    }

    pub async fn add_view_counts(&mut self, db: &DbSqlite) -> Result<()> {
        println!("add_view_counts: loading sites");
        self.load_sites(db).await?;
        println!("add_view_counts: sites loaded");
        let first_day = self.ym.first_day()?;
        let last_day = self.ym.last_day()?;

        // Hide Main Page from view count
        db.reset_main_page_view_count()?;

        let batch_size = ADD_VIEW_COUNTS_BATCH_SIZE;
        let mut found = true;
        let mut views_todo = vec![];
        while found {
            found = false;
            println!("add_view_counts: getting {batch_size} view counts");
            let rows = self.get_view_counts(db, batch_size)?;
            println!("add_view_counts: view counts retrieved");
            for vc in rows {
                self.add_view_counts_process_row(
                    vc,
                    db,
                    &mut found,
                    &mut views_todo,
                    &first_day,
                    &last_day,
                )
                .await;
            }
            if !views_todo.is_empty() {
                // println!("Preparing {} futures",views_todo.len());
                for views_todo_batch in views_todo.chunks(10) {
                    let futures: Vec<_> = views_todo_batch
                        .iter()
                        .map(|(server, title, first_day, last_day, _view_id)| {
                            self.get_total_monthly_page_views(server, title, first_day, last_day)
                        })
                        .collect();
                    let results: Vec<u64> = join_all(futures)
                        .await
                        .iter()
                        .map(|r| r.unwrap_or(0))
                        .collect();
                    // println!("Futures complete");
                    for (view_count, (_server, _title, _first_day, _last_day, view_id)) in
                        results.into_iter().zip(views_todo_batch.iter())
                    {
                        let _ = db.update_view_count(*view_id, view_count).await;
                    }
                }
                // println!("Updates complete");
            }
        }

        println!("add_view_counts: adding summary statistics");
        self.add_summary_statistics_to_sqlite3(db).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn add_view_counts_process_row(
        &mut self,
        vc: ViewCount,
        db: &DbSqlite,
        found: &mut bool,
        views_todo: &mut Vec<(String, String, String, String, usize)>,
        first_day: &String,
        last_day: &String,
    ) {
        let server = match vc.server {
            Some(server) => server,
            None => {
                let _ = db.view_done(vc.view_id, 2).await;
                return;
            }
        };
        *found = true;
        let site = match self.sites.get(&vc.site_id) {
            Some(site) => site,
            None => return,
        };
        let wiki = match self.site2wiki(site) {
            Some(wiki) => wiki,
            None => {
                let _ = db.view_done(vc.view_id, 3).await;
                return;
            }
        };
        match self
            .baglama
            .prefix_with_namespace(&vc.title, vc.namespace_id, &wiki)
            .await
        {
            Some(title) => {
                views_todo.push((
                    server.to_string(),
                    title.to_string(),
                    first_day.to_string(),
                    last_day.to_string(),
                    vc.view_id,
                ));
            }
            None => {
                let _ = db.view_done(vc.view_id, 4).await;
            }
        }
    }

    async fn add_summary_statistics_to_sqlite3(&self, db: &DbSqlite) -> Result<()> {
        let group_status_id = db.get_group_status_id(self)?;
        db.add_summary_statistics(group_status_id).await
    }

    async fn finalize_sqlite(&self, db: &DbSqlite) -> Result<()> {
        let group_status_id = db.get_group_status_id(self)?;
        let total_views = db.get_total_views(group_status_id)?;
        db.create_final_indices()?;
        let sqlite_filename = db.path_final();
        self.set_group_status("VIEW DATA COMPLETE", total_views, sqlite_filename)
            .await?;
        Ok(())
    }

    pub async fn set_group_status(
        &self,
        status: &str,
        total_views: usize,
        sqlite_filename: &str,
    ) -> Result<()> {
        if let Ok(mut mysql_connection) = self.baglama.get_tooldb_conn().await {
            let group_id = self.group_id.as_usize();
            let year = self.ym.year();
            let month = self.ym.month();
            let sql = "REPLACE INTO `group_status` (group_id,year,month,status,total_views,sqlite3) VALUES (:group_id,:year,:month,:status,:total_views,:sqlite_filename)";
            let _ = mysql_connection
                .exec_drop(
                    sql,
                    mysql_async::params! {group_id,year,month,status,total_views,sqlite_filename},
                )
                .await;
        }
        Ok(())
    }

    pub async fn create_sqlite(&mut self) -> Result<()> {
        let db = DbSqlite::new(self, self.baglama.clone())?;
        println!("{}-{}: seed_sqlite_file", self.group_id, self.ym);
        db.initialize(self.group_id(), self.ym().to_owned()).await?;
        println!("{}-{}: add_files_to_sqlite", self.group_id, self.ym);
        self.add_files_to_sqlite(&db).await?;
        println!("{}-{}: add_pages_to_sqlite", self.group_id, self.ym);
        self.add_pages_to_sqlite(&db).await?;
        println!("{}-{}: add_view_counts", self.group_id, self.ym);
        self.add_view_counts(&db).await?;
        println!("{}-{}: finalize_sqlite", self.group_id, self.ym);
        self.finalize_sqlite(&db).await?;
        println!("{}-{}: done!", self.group_id, self.ym);
        db.finalize(&self.ym)?;
        Ok(())
    }
}

// temp deact
// #[cfg(test)]
// mod tests {

//     use super::*;

//     #[tokio::test]
//     async fn test_get_total_monthly_page_views() {
//         let gd = GroupDate::new(1.into(), YearMonth::new(2022, 10));
//         let total = gd
//             .get_total_monthly_page_views(
//                 "en.wikipedia.org",
//                 "Eliza_Maria_Gordon-Cumming",
//                 "20221001",
//                 "20221031",
//             )
//             .await;
//         assert_eq!(total, Some(110));
//     }
// }
