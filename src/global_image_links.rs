use crate::Baglama2;
use anyhow::Result;
use mysql_async::from_row;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct GlobalImageLinks {
    pub wiki: String,
    pub page: usize,
    pub page_namespace_id: i32,
    //pub page_namespace: String,
    pub page_title: String,
    pub to: String,
}

impl GlobalImageLinks {
    pub async fn load(files: &[String], baglama: &Baglama2) -> Result<Vec<GlobalImageLinks>> {
        if files.is_empty() {
            return Ok(vec![]);
        }
        let placeholders = Baglama2::sql_placeholders(files.len());
        let sql = format!("SELECT gil_wiki,gil_page,gil_page_namespace_id,gil_page_namespace,gil_page_title,gil_to FROM `globalimagelinks` WHERE `gil_to` IN ({})",&placeholders);

        let max_attempts = 5;
        let mut last_error: Option<String> = None;
        for attempt in 0..max_attempts {
            if attempt > 0 {
                baglama.hold_on().await;
            }
            let mut mysql_commons_conn = match baglama.get_commons_conn().await {
                Ok(conn) => conn,
                Err(e) => {
                    last_error = Some(format!("Connection error: {e}"));
                    continue;
                }
            };
            let res = match mysql_commons_conn.exec_iter(&sql, files.to_owned()).await {
                Ok(res) => res,
                Err(e) => {
                    last_error = Some(format!("Query error: {e}"));
                    drop(mysql_commons_conn);
                    continue;
                }
            };
            match res.map_and_drop(from_row::<GlobalImageLinks>).await {
                Ok(ret) => return Ok(ret),
                Err(e) => {
                    last_error = Some(format!("Mapping error: {e}"));
                    drop(mysql_commons_conn);
                    continue;
                }
            }
        }
        Err(anyhow::anyhow!(
            "GlobalImageLinks::load failed after {max_attempts} attempts. Last error: {}",
            last_error.unwrap_or_else(|| "Unknown".to_string())
        ))
    }
}

impl FromRow for GlobalImageLinks {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let ret = Self {
            wiki: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page: row
                .get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page_namespace_id: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page_title: Baglama2::value2opt_string(
                row.as_ref(4)
                    .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            to: Baglama2::value2opt_string(
                row.as_ref(5)
                    .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
        };
        Ok(ret)
    }
}
