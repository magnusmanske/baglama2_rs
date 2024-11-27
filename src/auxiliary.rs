use crate::{baglama2::*, GroupId};
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct ViewCount {
    pub view_id: usize,
    pub title: String,
    pub namespace_id: i32,
    pub grok_code: Option<String>,
    pub server: Option<String>,
    pub done: i8,
    pub site_id: usize,
}

impl ViewCount {
    pub fn from_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            view_id: row.get(0)?,
            title: row.get(1)?,
            namespace_id: row.get(2)?,
            grok_code: row.get(3)?,
            server: row.get(4)?,
            done: row.get(5)?,
            site_id: row.get(6)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BaglamaGroup {
    id: GroupId,
}

impl BaglamaGroup {
    pub fn new(id: GroupId) -> Self {
        if id.is_valid() {
            panic!("Bad group ID: {id}");
        }
        Self { id }
    }

    pub fn id(&self) -> GroupId {
        self.id
    }
}

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

#[derive(Debug, Clone)]
pub struct RowGroup {
    pub id: usize,
    pub category: String,
    pub depth: usize,
    pub added_by: String,
    pub just_added: u8,
}

impl FromRow for RowGroup {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            id: row.get(0).unwrap(),
            category: Baglama2::value2opt_string(row.as_ref(1).unwrap()).unwrap(),
            depth: row.get(2).unwrap(),
            added_by: Baglama2::value2opt_string(row.as_ref(3).unwrap()).unwrap(),
            just_added: row.get(4).unwrap(),
        })
    }
}
