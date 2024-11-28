use crate::global_image_links::GlobalImageLinks;
use crate::{Baglama2, GroupDate, GroupId, Site, ViewCount, YearMonth};
use anyhow::{anyhow, Result};
use rusqlite::params_from_iter;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const SQLITE_DATA_TMP_PATH: &str = "/tmp";

#[derive(Debug, Clone)]
pub struct DbSqlite {
    path_final: String,
    path_tmp: String,
    connection: Arc<Mutex<Connection>>,
    baglama: Arc<Baglama2>,
    ym: YearMonth,
    group_id: GroupId,
}

impl DbSqlite {
    pub fn new(gd: &GroupDate, baglama: Arc<Baglama2>) -> Result<Self> {
        let path_final = Self::construct_sqlite3_filename(gd, &baglama)?;
        let mut path_tmp = Self::construct_sqlite3_temporary_filename(gd)?;
        if std::path::Path::new(&path_final).exists() && !std::path::Path::new(&path_tmp).exists() {
            path_tmp = path_final.clone();
        }
        println!(
            "{}: {} [ {path_tmp} => {path_final} ]",
            &gd.ym(),
            gd.group_id()
        );
        if std::path::Path::new(&path_tmp).exists() {
            let _ = std::fs::remove_file(&path_tmp);
        }
        let connection = Arc::new(Mutex::new(Connection::open(&path_tmp)?));
        Ok(Self {
            path_final,
            path_tmp,
            connection,
            baglama,
            ym: gd.ym().to_owned(),
            group_id: gd.group_id(),
        })
    }

    /// Returns a mutex lock on the sqlite connection
    fn conn(&self) -> std::sync::MutexGuard<Connection> {
        self.connection.lock().unwrap()
    }

    pub fn path_final(&self) -> &str {
        &self.path_final
    }

    pub fn finalize(&self, ym: &YearMonth) -> Result<()> {
        if self.path_tmp != self.path_final {
            let _ = ym.make_production_directory(&self.baglama);
            std::fs::copy(&self.path_tmp, &self.path_final)?;
            let _ = std::fs::remove_file(&self.path_tmp);
        }
        Ok(())
    }

    pub fn load_sites(&self) -> Result<Vec<Site>> {
        let sql = "SELECT id,grok_code,server,giu_code,project,language,name FROM `sites`";
        let sites = self
            .conn()
            .prepare(sql)?
            .query_map([], Site::from_sqlite_row)?
            .flatten()
            .collect();
        Ok(sites)
    }

    pub fn get_view_counts(&self, batch_size: usize) -> Result<Vec<ViewCount>, rusqlite::Error> {
        let sql = format!("SELECT DISTINCT `views`.`id` AS id,title,namespace_id,grok_code,server,done,`views`.`site` AS site_id FROM `views`,`sites` WHERE `done`=0 AND `sites`.`id`=`views`.`site` LIMIT {batch_size}");
        let ret: Vec<ViewCount> = self
            .conn()
            .prepare(&sql)?
            .query_map([], ViewCount::from_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(ret)
    }

    pub fn get_group_status_id(&self) -> Result<usize> {
        let group_id = self.group_id.to_owned();
        let year = self.ym.year();
        let month = self.ym.month();
        let sql = "SELECT `id` FROM `group_status` WHERE `group_id`=? AND `year`=? AND `month`=?";
        let group_status_id: usize = self
            .conn()
            .prepare(sql)?
            .query_map((group_id.as_usize(), year, month), |row| row.get(0))?
            .next()
            .ok_or(anyhow!("No group_status for group {group_id}"))??;
        Ok(group_status_id)
    }

    pub fn get_total_views(&self, group_status_id: usize) -> Result<usize> {
        let sql = "SELECT ifnull(total_views,0) FROM group_status WHERE id=?";
        let total_views: usize = self
            .conn()
            .prepare(sql)?
            .query_map([group_status_id], |row| row.get(0))?
            .next()
            .unwrap_or(Ok(0))?;
        Ok(total_views)
    }

    pub fn create_final_indices(&self) -> Result<()> {
        self.conn().execute(
            "CREATE INDEX `views_views_site_done` ON `views` (`site`,`done`,`views`)",
            (),
        )?;
        self.conn()
            .execute("CREATE INDEX `g2v_view_id` ON `group2view` (`view_id`)", ())?;
        Ok(())
    }

    pub fn delete_all_files(&self) -> Result<()> {
        // DO NOT IMPLEMENT THIS FOR MYSQL!!
        self.conn().execute("DELETE FROM `files`", ())?;
        Ok(())
    }

    pub fn delete_views(&self) -> Result<()> {
        // DO NOT IMPLEMENT THIS FOR MYSQL!!
        self.conn().execute("DELETE FROM `views`", ())?;
        Ok(())
    }

    pub fn delete_group2view(&self) -> Result<()> {
        // DO NOT IMPLEMENT THIS FOR MYSQL!!
        self.conn().execute("DELETE FROM `group2view`", ())?;
        Ok(())
    }

    pub fn load_files_batch(&self, offset: usize, batch_size: usize) -> Result<Vec<String>> {
        let sql = format!("SELECT `filename` FROM `files` LIMIT {batch_size} OFFSET {offset}");
        let files: Vec<String> = self
            .conn()
            .prepare(&sql)?
            .query_map([], |row| row.get(0))?
            .filter_map(|x| x.ok()) // TODO something more elegant?
            .collect();
        Ok(files)
    }

    pub async fn add_views_for_files(&self, all_files: &[String], gd: &GroupDate) -> Result<()> {
        if all_files.is_empty() {
            return Ok(());
        }

        let group_status_id = self.get_group_status_id()?;

        const CHUNK_SIZE: usize = 3000;
        let mut chunk_num = 0;
        for files in all_files.chunks(CHUNK_SIZE) {
            chunk_num += 1;
            println!("add_views_for_files_to_sqlite: starting chunk {chunk_num} ({CHUNK_SIZE} of {} files total)",all_files.len());
            let globalimagelinks = GlobalImageLinks::load(files, &self.baglama).await?;
            println!("add_views_for_files_to_sqlite: globalimagelinks done");
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
                self.add_views_batch_for_files_to_sqlite(sql_values, parts, group_status_id)?;
            }

            println!("add_views_for_files_to_sqlite: batch done");
        }
        println!("add_views_for_files_to_sqlite: all batches done");

        Ok(())
    }

    pub fn reset_main_page_view_count(&self) -> Result<()> {
        // TODO for all wikis?
        let sql = "UPDATE views SET views=0 WHERE title='Main_Page'";
        self.conn().execute(sql, ())?;
        Ok(())
    }

    pub async fn add_summary_statistics(&self, group_status_id: usize) -> Result<()> {
        self.conn()
            .execute("CREATE INDEX `views_site` ON `views` (site)", ())?;
        self.conn().execute("DELETE FROM `gs2site`", ())?;
        self.conn().execute("INSERT INTO `gs2site` SELECT sites.id,?1,sites.id,COUNT(DISTINCT page_id),SUM(views) FROM `views`,`sites` WHERE views.site=sites.id GROUP BY sites.id",rusqlite::params![group_status_id])?;
        self.conn().execute("UPDATE group_status SET status='VIEW DATA COMPLETE',total_views=(SELECT sum(views) FROM gs2site) WHERE id=?1",rusqlite::params![group_status_id])?;
        Ok(())
    }

    pub async fn update_view_count(&self, view_id: usize, view_count: u64) -> Result<usize> {
        self.conn()
            .execute(
                "UPDATE `views` SET `done`=1,`views`=?1 WHERE `id`=?2",
                rusqlite::params![view_count, view_id],
            )
            .map_err(|e| anyhow!(e))
    }

    pub async fn view_done(&self, view_id: usize, done: u8) -> Result<usize> {
        self.conn()
            .execute(
                "UPDATE `views` SET `done`=?1,`views`=0 WHERE `id`=?2",
                rusqlite::params![done, view_id],
            )
            .map_err(|e| anyhow!(e))
    }

    fn add_views_batch_for_files_to_sqlite(
        &self,
        sql_values: Vec<String>,
        parts: Vec<(usize, String, String)>,
        group_status_id: usize,
    ) -> Result<()> {
        let sql = "INSERT OR IGNORE INTO `views` (site,title,month,year,done,namespace_id,page_id,views) VALUES ".to_string() + &sql_values.join(",");
        let titles = parts.iter().map(|p| p.1.to_owned());
        self.conn().execute(&sql, params_from_iter(titles))?;
        let site_titles: Vec<String> = parts.iter().map(|part| part.1.to_owned()).collect();
        let placeholders: Vec<String> = parts
            .iter()
            .map(|part| format!("(`site`={} AND `title`=?)", part.0))
            .collect();
        let sql = "SELECT id,site,title FROM `views` WHERE ".to_string()
            + &placeholders.join(" OR ").to_string();
        let viewid_site_id_title: Vec<(usize, usize, String)> = self
            .conn()
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
            let sql = "INSERT OR IGNORE INTO `group2view` (group_status_id,view_id,image) VALUES "
                .to_string()
                + &values.join(",").to_string();
            // println!("{sql}\n{images:?}\n");
            self.conn().execute(&sql, params_from_iter(images))?;
        }
        Ok(())
    }

    pub fn file_insert_batch_size(&self) -> usize {
        450 // sqlite3 limit is 500
    }

    pub fn insert_files_batch(&self, batch: &[String]) -> Result<()> {
        let questionmarks = ["(?)"].repeat(batch.len()).join(",");
        let sql = format!("INSERT INTO `files` (`filename`) VALUES {questionmarks}");
        self.conn()
            .execute(&sql, rusqlite::params_from_iter(batch.iter()))?;
        Ok(())
    }

    pub async fn initialize(&self, group_id: GroupId, ym: YearMonth) -> Result<()> {
        let sql = std::fs::read_to_string(self.baglama.sqlite_schema_file())?;
        self.execute_batch(&sql)?;
        self.seed_file_sites().await?;
        self.seed_file_groups(&group_id).await?;
        self.group_status(&group_id, &ym).await?;
        self.conn().execute(
            "UPDATE `group_status` SET `status`='',`total_views`=null,`file`=null,`sqlite3`=null",
            (),
        )?;
        Ok(())
    }

    pub async fn seed_file_sites(&self) -> Result<()> {
        // DO NOT IMPLEMENT THIS FOR MYSQL
        self.conn().execute("DELETE FROM `sites`", ())?;
        let sites = self.baglama.get_sites().await?;
        for site in &sites {
            let sql = "INSERT INTO `sites` (id,grok_code,server,giu_code,project,language,name) VALUES (?,?,?,?,?,?,?)" ;
            self.conn().execute(
                sql,
                rusqlite::params![
                    site.id(),
                    site.grok_code(),
                    site.server(),
                    site.giu_code(),
                    site.project(),
                    site.language(),
                    site.name()
                ],
            )?;
        }
        Ok(())
    }

    pub async fn seed_file_groups(&self, group_id: &GroupId) -> Result<()> {
        // DO NOT IMPLEMENT THIS FOR MYSQL
        self.conn().execute("DELETE FROM `groups`", ())?;
        let groups = self.baglama.get_group(group_id).await?;
        if let Some(group) = groups {
            let sql =
                "INSERT INTO `groups` (id,category,depth,added_by,just_added) VALUES (?,?,?,?,?)";
            self.conn().execute(
                sql,
                rusqlite::params![
                    group.id(),
                    group.category(),
                    group.depth(),
                    group.added_by(),
                    group.just_added(),
                ],
            )?;
        }
        Ok(())
    }

    pub async fn group_status(&self, group_id: &GroupId, ym: &YearMonth) -> Result<()> {
        self.conn().execute("DELETE FROM `group_status`", ())?;
        let group_status = self.baglama.get_group_status(group_id, ym).await?;
        if let Some(gs) = group_status {
            let sql = "INSERT INTO `group_status` (id,group_id,year,month,status,total_views,file,sqlite3) VALUES (?,?,?,?,?,?,?,?)" ;
            self.conn().execute(
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
            self.conn().execute(
                sql,
                rusqlite::params![group_id.as_usize(), ym.year(), ym.month()],
            )?;
        }
        Ok(())
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.conn().execute(sql, [])?;
        Ok(())
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn().execute_batch(sql)?;
        Ok(())
    }

    fn construct_sqlite3_filename(gd: &GroupDate, baglama: &Baglama2) -> Result<String> {
        let dir = gd.ym().make_production_directory(baglama)?;
        let file_name = format!("{dir}/{}.sqlite", gd.group_id());
        Ok(file_name)
    }

    fn construct_sqlite3_temporary_filename(gd: &GroupDate) -> Result<String> {
        std::fs::create_dir_all(SQLITE_DATA_TMP_PATH)?;
        Ok(format!(
            "{}/{}.{}.sqlite3",
            SQLITE_DATA_TMP_PATH,
            gd.ym(),
            gd.group_id()
        ))
    }
}
