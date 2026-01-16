// use anyhow::Result;
use mysql_async::prelude::*;

use crate::baglama2::Baglama2;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: Option<usize>,
    pub site_id: usize,
    pub title: String,
    pub namespace_id: i32,
}

impl Page {
    pub fn sql_fields() -> &'static str {
        "id,site,title,namespace_id"
    }

    pub fn new(site_id: usize, title: String, namespace_id: i32) -> Self {
        Self {
            id: None,
            site_id,
            title,
            namespace_id,
        }
    }
}

impl FromRow for Page {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let title = Baglama2::value2opt_string(
            row.as_ref(2)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
        )
        .ok_or_else(|| mysql_async::FromRowError(row.clone()))?;

        Ok(Self {
            id: row
                .get("id")
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            site_id: row
                .get("site")
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            title,
            namespace_id: row
                .get("namespace_id")
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}
