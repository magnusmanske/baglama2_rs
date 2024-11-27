use crate::baglama2::*;
use crate::global_image_links::GlobalImageLinks;
use crate::GroupId;
use crate::Site;
use crate::ViewCount;
use crate::YearMonth;
use anyhow::{anyhow, Result};
use futures::future::join_all;
use mysql_async::from_row;
use mysql_async::prelude::*;
use rusqlite::{params_from_iter, Connection};
use serde_json::Value;
use std::collections::HashMap;

const SQLITE_DATA_TMP_PATH: &str = "/tmp";
const ADD_VIEW_COUNTS_BATCH_SIZE: usize = 3000;

#[derive(Debug, Clone)]
pub struct GroupDate {
    group_id: GroupId,
    ym: YearMonth,
    sites: HashMap<usize, Site>,
    wiki2site_id: HashMap<String, usize>,
}

impl GroupDate {
    pub fn new(id: GroupId, ym: YearMonth) -> Self {
        Self {
            group_id: id,
            ym,
            sites: HashMap::new(),
            wiki2site_id: HashMap::new(),
        }
    }

    pub fn construct_sqlite3_filename(&self, baglama: &Baglama2) -> Result<String> {
        let dir = self.ym.make_production_directory(baglama)?;
        let file_name = format!("{dir}/{}.sqlite", self.group_id);
        Ok(file_name)
    }

    pub fn construct_sqlite3_temporary_filename(&self) -> Result<String> {
        std::fs::create_dir_all(SQLITE_DATA_TMP_PATH)?;
        Ok(format!(
            "{}/{}.{}.sqlite3",
            SQLITE_DATA_TMP_PATH, &self.ym, self.group_id
        ))
    }

    async fn load_sites_from_sqlite(&mut self, conn: &mut Connection) -> Result<()> {
        let sql = "SELECT id,grok_code,server,giu_code,project,language,name FROM `sites`";
        let mut stmt = conn.prepare(sql)?;
        let sites = stmt.query_map([], Site::from_sqlite_row)?;
        for site in sites.flatten() {
            if let Some(wiki) = self.site2wiki(&site) {
                self.wiki2site_id.insert(wiki, site.id());
                self.sites.insert(site.id(), site);
            }
        }
        Ok(())
    }

    fn get_view_counts(
        &self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> Result<Vec<ViewCount>, rusqlite::Error> {
        let sql = format!("SELECT DISTINCT `views`.`id` AS id,title,namespace_id,grok_code,server,done,`views`.`site` AS site_id FROM `views`,`sites` WHERE `done`=0 AND `sites`.`id`=`views`.`site` LIMIT {batch_size}");
        let conn2 = conn;
        let mut stmt = conn2.prepare(&sql)?;
        let rows = stmt.query_map([], ViewCount::from_row)?;
        let ret: Vec<ViewCount> = rows.into_iter().filter_map(|row| row.ok()).collect();
        Ok(ret)
    }

    async fn group_status_id(&self, conn: &mut Connection) -> Result<usize> {
        let group_id = self.group_id;
        let year = self.ym.year();
        let month = self.ym.month();
        let sql = "SELECT `id` FROM `group_status` WHERE `group_id`=? AND `year`=? AND `month`=?";
        let group_status_id: usize = conn
            .prepare(sql)?
            .query_map((group_id.as_usize(), year, month), |row| row.get(0))?
            .next()
            .ok_or(anyhow!("No group_status for group {group_id}"))??;
        Ok(group_status_id)
    }

    fn get_site_for_wiki(&self, wiki: &str) -> Option<&Site> {
        match self.wiki2site_id.get(wiki) {
            Some(site_id) => self.sites.get(site_id),
            None => None,
        }
    }

    async fn get_globalimagelinks(
        &self,
        files: &[String],
        baglama: &Baglama2,
    ) -> Result<Vec<GlobalImageLinks>> {
        if files.is_empty() {
            return Ok(vec![]);
        }
        let placeholders = Baglama2::sql_placeholders(files.len());
        let sql = format!("SELECT gil_wiki,gil_page,gil_page_namespace_id,gil_page_namespace,gil_page_title,gil_to FROM `globalimagelinks` WHERE `gil_to` IN ({})",&placeholders);

        let max_attempts = 5;
        let mut current_attempt = 0;
        let mut ret = vec![];
        loop {
            if current_attempt >= max_attempts {
                break; // TODO error?
            }
            if current_attempt > 0 {
                baglama.hold_on();
            }
            current_attempt += 1;
            let mut conn = match baglama.get_commons_conn().await {
                Ok(conn) => conn,
                _ => continue,
            };
            let res = match conn.exec_iter(&sql, files.to_owned()).await {
                Ok(res) => res,
                _ => {
                    drop(conn);
                    continue;
                }
            };
            ret = match res.map_and_drop(from_row::<GlobalImageLinks>).await {
                Ok(ret) => ret,
                _ => {
                    drop(conn);
                    continue;
                }
            };
            break;
        }
        Ok(ret)
    }

    async fn add_views_for_files_to_sqlite(
        &self,
        conn: &mut Connection,
        all_files: &[String],
        baglama: &Baglama2,
    ) -> Result<()> {
        if all_files.is_empty() {
            return Ok(());
        }
        let group_status_id = self.group_status_id(conn).await?;
        const CHUNK_SIZE: usize = 3000;
        let mut chunk_num = 0;
        for files in all_files.chunks(CHUNK_SIZE) {
            chunk_num += 1;
            println!("add_views_for_files_to_sqlite: starting chunk {chunk_num} ({CHUNK_SIZE} of {} files total)",all_files.len());
            let globalimagelinks = self.get_globalimagelinks(files, baglama).await?;
            println!("add_views_for_files_to_sqlite: globalimagelinks done");
            let mut sql_values = vec![];
            let mut parts = vec![];
            for gil in &globalimagelinks {
                let site = match self.get_site_for_wiki(&gil.wiki) {
                    Some(site) => site,
                    None => {
                        //println!("Unknown wiki: {}",&gil.wiki);
                        continue;
                    }
                };

                let site_id = site.id();
                let title = &gil.page_title;
                let month = self.ym.month();
                let year = self.ym.year();
                let done = 0;
                let namespace_id = gil.page_namespace_id;
                let page_id = gil.page;
                let views = 0;

                let sql_value =
                    format!("({site_id},?,{month},{year},{done},{namespace_id},{page_id},{views})");
                sql_values.push(sql_value);
                let part = (site_id, title.to_owned(), gil.to.to_owned());
                parts.push(part);
            }

            if !parts.is_empty() {
                let sql = "INSERT OR IGNORE INTO `views` (site,title,month,year,done,namespace_id,page_id,views) VALUES ".to_string() + &sql_values.join(",");
                let titles = parts.iter().map(|p| p.1.to_owned());
                // println!("{sql}\n");
                conn.execute(&sql, params_from_iter(titles))?;

                let site_titles: Vec<String> = parts.iter().map(|part| part.1.to_owned()).collect();
                let placeholders: Vec<String> = parts
                    .iter()
                    .map(|part| format!("(`site`={} AND `title`=?)", part.0))
                    .collect();
                let sql = "SELECT id,site,title FROM `views` WHERE ".to_string()
                    + &placeholders.join(" OR ").to_string();
                // println!("{sql}\n{site_titles:?}");
                // conn.execute(&sql, params_from_iter(site_titles))?;
                let viewid_site_id_title: Vec<(usize, usize, String)> = conn
                    .prepare(&sql)?
                    .query_map(params_from_iter(site_titles), |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })?
                    .filter_map(|x| x.ok())
                    .collect();
                let siteid_title_viewid: HashMap<(usize, String), usize> = viewid_site_id_title
                    .into_iter()
                    .map(|(view_id, site_id, title)| ((site_id, title.to_owned()), view_id))
                    .collect();
                // println!("{:?}",&siteid_title_viewid);

                let mut values = vec![];
                let mut images = vec![];
                for (site_id, title, image) in &parts {
                    let view_id = match siteid_title_viewid.get(&(*site_id, title.to_owned())) {
                        Some(id) => id,
                        None => {
                            println!("{site_id}/{title} not found, odd");
                            continue;
                        }
                    };
                    values.push(format!("({group_status_id},{view_id},?)"));
                    images.push(image.to_owned());
                }

                if !values.is_empty() {
                    let sql =
                        "INSERT OR IGNORE INTO `group2view` (group_status_id,view_id,image) VALUES "
                            .to_string() + &values.join(",").to_string();
                    // println!("{sql}\n{images:?}\n");
                    conn.execute(&sql, params_from_iter(images))?;
                }
            }

            println!("add_views_for_files_to_sqlite: batch done");
        }
        println!("add_views_for_files_to_sqlite: all batches done");

        Ok(())
    }

    async fn get_files_from_commons_category_tree(
        &self,
        category: &str,
        depth: usize,
        baglama: &Baglama2,
    ) -> Result<Vec<String>> {
        baglama.get_pages_in_category(category, depth, 6).await
    }

    pub async fn seed_sqlite_file(&self, conn: &mut Connection, baglama: &Baglama2) -> Result<()> {
        let sql = std::fs::read_to_string(baglama.sqlite_schema_file())?;
        conn.execute_batch(&sql)?;

        // sites
        conn.execute("DELETE FROM `sites`", ())?;
        let sites = baglama.get_sites().await?;
        for site in &sites {
            let sql = "INSERT INTO `sites` (id,grok_code,server,giu_code,project,language,name) VALUES (?,?,?,?,?,?,?)" ;
            conn.execute(
                sql,
                rusqlite::params![
                    site.id(),
                    site.grok_code,
                    site.server,
                    site.giu_code,
                    site.project(),
                    site.language(),
                    site.name
                ],
            )?;
        }

        // groups
        conn.execute("DELETE FROM `groups`", ())?;
        let groups = baglama.get_group(self.group_id).await?;
        if let Some(group) = groups {
            let sql =
                "INSERT INTO `groups` (id,category,depth,added_by,just_added) VALUES (?,?,?,?,?)";
            conn.execute(
                sql,
                rusqlite::params![
                    group.id,
                    group.category,
                    group.depth,
                    group.added_by,
                    group.just_added
                ],
            )?;
        }

        // group_status
        conn.execute("DELETE FROM `group_status`", ())?;
        let group_status = baglama.get_group_status(self.group_id, &self.ym).await?;
        if let Some(gs) = group_status {
            let sql = "INSERT INTO `group_status` (id,group_id,year,month,status,total_views,file,sqlite3) VALUES (?,?,?,?,?,?,?,?)" ;
            conn.execute(
                sql,
                rusqlite::params![
                    gs.id,
                    gs.group_id,
                    gs.year,
                    gs.month,
                    gs.status,
                    gs.total_views,
                    gs.file,
                    gs.sqlite3
                ],
            )?;
        } else {
            let sql = "INSERT INTO `group_status` (group_id,year,month) VALUES (?,?,?)";
            conn.execute(
                sql,
                rusqlite::params![self.group_id.as_usize(), self.ym.year(), self.ym.month()],
            )?;
        }

        conn.execute(
            "UPDATE `group_status` SET `status`='',`total_views`=null,`file`=null,`sqlite3`=null",
            (),
        )?;
        Ok(())
    }

    pub async fn add_files_to_sqlite(
        &self,
        conn: &mut Connection,
        baglama: &Baglama2,
    ) -> Result<()> {
        let sql = "SELECT `category`,`depth` FROM `groups` WHERE `id`=?";
        let (category, depth): (String, usize) = conn
            .prepare(sql)?
            .query_map([self.group_id.as_usize()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .next()
            .ok_or(anyhow!(sql))??;
        conn.execute("DELETE FROM `files`", ())?;

        // Get files in category tree from Commons
        let batch_size: usize = 450; // sqlite3 limit is 500
        let files = self
            .get_files_from_commons_category_tree(&category, depth, baglama)
            .await?;
        let mut parts = Vec::with_capacity(batch_size);
        for file in &files {
            parts.push(file.to_owned());
            if parts.len() >= batch_size {
                let questionmarks = ["(?)"].repeat(parts.len());
                let sql = format!(
                    "INSERT INTO `files` (`filename`) VALUES {}",
                    questionmarks.join(",")
                );
                conn.execute(&sql, rusqlite::params_from_iter(parts.iter()))?;
                parts.clear();
            }
        }
        if !parts.is_empty() {
            let questionmarks = ["(?)"].repeat(parts.len());
            let sql = format!(
                "INSERT INTO `files` (`filename`) VALUES {}",
                questionmarks.join(",")
            );
            conn.execute(&sql, rusqlite::params_from_iter(parts.iter()))?;
        }
        Ok(())
    }

    pub async fn add_pages_to_sqlite(
        &mut self,
        conn: &mut Connection,
        baglama: &Baglama2,
    ) -> Result<()> {
        self.load_sites_from_sqlite(conn).await?;
        conn.execute("DELETE FROM `views`", ())?;
        conn.execute("DELETE FROM `group2view`", ())?;
        let mut offset: usize = 0;
        const BATCH_SIZE: usize = 10000;
        loop {
            let sql = format!("SELECT `filename` FROM `files` LIMIT {BATCH_SIZE} OFFSET {offset}");
            let files: Vec<String> = conn
                .prepare(&sql)?
                .query_map([], |row| row.get(0))?
                .filter_map(|x| x.ok()) // TODO something more elegant?
                .collect();
            let _ = self
                .add_views_for_files_to_sqlite(conn, &files, baglama)
                .await;
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

    async fn view_done(&self, view_id: usize, done: u8, conn: &mut Connection) -> Result<usize> {
        conn.execute(
            "UPDATE `views` SET `done`=?1,`views`=0 WHERE `id`=?2",
            rusqlite::params![done, view_id],
        )
        .map_err(|e| anyhow!(e))
    }

    async fn update_view_count(
        &self,
        view_id: usize,
        view_count: u64,
        conn: &mut Connection,
    ) -> Result<usize> {
        conn.execute(
            "UPDATE `views` SET `done`=1,`views`=?1 WHERE `id`=?2",
            rusqlite::params![view_count, view_id],
        )
        .map_err(|e| anyhow!(e))
    }

    pub async fn add_view_counts_to_sqlite(
        &mut self,
        conn: &mut Connection,
        baglama: &Baglama2,
    ) -> Result<()> {
        println!("add_view_counts_to_sqlite: loading sites");
        self.load_sites_from_sqlite(conn).await?;
        println!("add_view_counts_to_sqlite: sites loaded");
        let first_day = self.ym.first_day()?;
        let last_day = self.ym.last_day()?;

        // Hide Main Page from view count
        // TODO for all wikis?
        let sql = "UPDATE views SET views=0 WHERE title='Main_Page'";
        conn.execute(sql, ())?;

        let batch_size = ADD_VIEW_COUNTS_BATCH_SIZE;
        let mut found = true;
        let mut views_todo = vec![];
        while found {
            found = false;
            println!("add_view_counts_to_sqlite: getting {batch_size} view counts");
            let rows = self.get_view_counts(conn, batch_size)?;
            println!("add_view_counts_to_sqlite: view counts retrieved");
            for vc in rows {
                let server = match vc.server {
                    Some(server) => server,
                    None => {
                        let _ = self.view_done(vc.view_id, 2, conn).await;
                        continue;
                    }
                };
                found = true;
                let site = match self.sites.get(&vc.site_id) {
                    Some(site) => site,
                    None => continue,
                };
                let wiki = match self.site2wiki(site) {
                    Some(wiki) => wiki,
                    None => {
                        let _ = self.view_done(vc.view_id, 3, conn).await;
                        continue;
                    }
                };
                match baglama
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
                        // let view_count = self.get_total_monthly_page_views(&server, &title, &first_day, &last_day).await.unwrap_or(0);
                        // let _ = self.update_view_count(vc.view_id, view_count, &conn).await;
                    }
                    None => {
                        let _ = self.view_done(vc.view_id, 4, conn).await;
                    }
                }
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
                        let _ = self.update_view_count(*view_id, view_count, conn).await;
                    }
                }
                // println!("Updates complete");
            }
        }

        println!("add_view_counts_to_sqlite: adding summary statistics");
        self.add_summary_statistics_to_sqlite3(conn).await?;
        Ok(())
    }

    async fn add_summary_statistics_to_sqlite3(&self, conn: &mut Connection) -> Result<()> {
        let group_status_id = self.group_status_id(conn).await?;
        conn.execute("CREATE INDEX `views_site` ON `views` (site)", ())?;
        conn.execute("DELETE FROM `gs2site`", ())?;
        conn.execute("INSERT INTO `gs2site` SELECT sites.id,?1,sites.id,COUNT(DISTINCT page_id),SUM(views) FROM `views`,`sites` WHERE views.site=sites.id GROUP BY sites.id",rusqlite::params![group_status_id])?;
        conn.execute("UPDATE group_status SET status='VIEW DATA COMPLETE',total_views=(SELECT sum(views) FROM gs2site) WHERE id=?1",rusqlite::params![group_status_id])?;
        Ok(())
    }

    async fn finalize_sqlite(
        &self,
        conn: &mut Connection,
        sqlite_filename: &str,
        baglama: &Baglama2,
    ) -> Result<()> {
        let sql = "SELECT id FROM group_status WHERE `group_id`=?";
        let group_status_id: usize = conn
            .prepare(sql)?
            .query_map([self.group_id.as_usize()], |row| row.get(0))?
            .next()
            .ok_or(anyhow!(sql))??;

        let sql = "SELECT ifnull(total_views,0) FROM group_status WHERE id=?";
        let total_views: usize = conn
            .prepare(sql)?
            .query_map([group_status_id], |row| row.get(0))?
            .next()
            .unwrap_or(Ok(0))?;

        conn.execute(
            "CREATE INDEX `views_views_site_done` ON `views` (`site`,`done`,`views`)",
            (),
        )?;
        conn.execute("CREATE INDEX `g2v_view_id` ON `group2view` (`view_id`)", ())?;
        self.set_group_status("VIEW DATA COMPLETE", total_views, sqlite_filename, baglama)
            .await?;
        Ok(())
    }

    pub async fn set_group_status(
        &self,
        status: &str,
        total_views: usize,
        sqlite_filename: &str,
        baglama: &Baglama2,
    ) -> Result<()> {
        if let Ok(mut conn) = baglama.get_tooldb_conn().await {
            let group_id = self.group_id.as_usize();
            let year = self.ym.year();
            let month = self.ym.month();
            let sql = "REPLACE INTO `group_status` (group_id,year,month,status,total_views,sqlite3) VALUES (:group_id,:year,:month,:status,:total_views,:sqlite_filename)";
            let _ = conn
                .exec_drop(
                    sql,
                    mysql_async::params! {group_id,year,month,status,total_views,sqlite_filename},
                )
                .await;
        }
        Ok(())
    }

    pub async fn create_sqlite(&mut self, baglama: &Baglama2) -> Result<()> {
        let fn_final = self.construct_sqlite3_filename(baglama)?;
        let mut fn_work = self.construct_sqlite3_temporary_filename()?;
        if std::path::Path::new(&fn_final).exists() && !std::path::Path::new(&fn_work).exists() {
            fn_work = fn_final.clone();
        }
        println!(
            "{}: {} [ {fn_work} => {fn_final} ]",
            &self.ym, self.group_id
        );
        if std::path::Path::new(&fn_work).exists() {
            let _ = std::fs::remove_file(&fn_work);
        }
        let mut conn = Connection::open(&fn_work)?;
        println!("{}-{}: seed_sqlite_file", self.group_id, self.ym);
        self.seed_sqlite_file(&mut conn, baglama).await?;
        println!("{}-{}: add_files_to_sqlite", self.group_id, self.ym);
        self.add_files_to_sqlite(&mut conn, baglama).await?;
        println!("{}-{}: add_pages_to_sqlite", self.group_id, self.ym);
        self.add_pages_to_sqlite(&mut conn, baglama).await?;
        println!("{}-{}: add_view_counts_to_sqlite", self.group_id, self.ym);
        self.add_view_counts_to_sqlite(&mut conn, baglama).await?;
        println!("{}-{}: finalize_sqlite", self.group_id, self.ym);
        self.finalize_sqlite(&mut conn, &fn_final, baglama).await?;
        println!("{}-{}: done!", self.group_id, self.ym);
        drop(conn);
        if fn_work != fn_final {
            let _ = self.ym.make_production_directory(baglama);
            std::fs::copy(&fn_work, &fn_final)?;
            let _ = std::fs::remove_file(&fn_work);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_get_total_monthly_page_views() {
        let gd = GroupDate::new(1.into(), YearMonth::new(2022, 10));
        let total = gd
            .get_total_monthly_page_views(
                "en.wikipedia.org",
                "Eliza_Maria_Gordon-Cumming",
                "20221001",
                "20221031",
            )
            .await;
        assert_eq!(total, Some(110));
    }
}
