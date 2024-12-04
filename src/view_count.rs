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

impl FromRow for ViewCount {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            view_id: row.get(0).unwrap(),
            title: row.get(1).unwrap(),
            namespace_id: row.get(2).unwrap(),
            grok_code: row.get(3).unwrap(),
            server: row.get(4).unwrap(),
            done: row.get(5).unwrap(),
            site_id: row.get(6).unwrap(),
        })
    }
}
