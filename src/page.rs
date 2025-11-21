// use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: usize,
    pub site_id: usize,
    pub title: String,
    pub namespace_id: usize,
}

impl Page {
    pub fn sql_fields() -> &'static str {
        "id,site,title,namespace_id"
    }
}

impl FromRow for Page {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            id: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            site_id: row
                .get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            title: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            namespace_id: row
                .get(3)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}
