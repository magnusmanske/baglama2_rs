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
            id: row.get(0).unwrap(),
            group_id: row.get(1).unwrap(),
            year: row.get(2).unwrap(),
            month: row.get(3).unwrap(),
            status: row.get(4).unwrap(),
            total_views: row.get(5).unwrap(),
            file: row.get(6).unwrap(),
            sqlite3: row.get(7).unwrap(),
        })
    }
}
