use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupStatus {
    pub id: usize,
    pub group_id: usize,
    pub year: i32,
    pub month: u32,
    pub status: String,
    pub total_views: Option<usize>,
    pub file: Option<String>,
    pub sqlite3: Option<String>,
}

impl FromRow for RowGroupStatus {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            id: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            group_id: row
                .get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            year: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            month: row
                .get(3)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            status: row
                .get(4)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            total_views: row.get(5).unwrap(),
            file: row.get(6).unwrap(),
            sqlite3: row.get(7).unwrap(),
        })
    }
}
