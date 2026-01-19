use mysql_async::prelude::*;

use crate::{Baglama2, DbId};

#[derive(Debug, Clone)]
pub struct ViewCount {
    pub view_id: DbId,
    pub title: String,
    pub namespace_id: i32,
    pub grok_code: Option<String>,
    pub server: Option<String>,
    pub done: i8,
    pub site_id: DbId,
}

impl ViewCount {
    pub fn from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        let view_id: isize = row.get(0)?;
        let site_id: isize = row.get(6)?;
        Ok(Self {
            view_id: view_id as DbId,
            title: row.get(1)?,
            namespace_id: row.get(2)?,
            grok_code: row.get(3)?,
            server: row.get(4)?,
            done: row.get(5)?,
            site_id: site_id as DbId,
        })
    }
}

impl FromRow for ViewCount {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let title = row
            .as_ref(1)
            .ok_or_else(|| mysql_async::FromRowError(row.clone()))
            .unwrap();
        let title = Baglama2::value2opt_string(title)
            .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?;
        Ok(Self {
            view_id: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            title,
            namespace_id: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            grok_code: row
                .get(3)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            server: row
                .get(4)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            done: row
                .get(5)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            site_id: row
                .get(6)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}
