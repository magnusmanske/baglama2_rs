use crate::{Baglama2, DbId};
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct Site {
    id: DbId,
    grok_code: Option<String>,
    server: Option<String>,
    giu_code: Option<String>,
    project: Option<String>,
    language: Option<String>,
    name: Option<String>,
}

impl Site {
    pub fn from_sqlite_row(row: &rusqlite::Row) -> Result<Self, rusqlite::Error> {
        let id: isize = row.get(0)?;
        Ok(Self {
            id: id as DbId,
            grok_code: row.get(1)?,
            server: row.get(2)?,
            giu_code: row.get(3)?,
            project: row.get(4)?,
            language: row.get(5)?,
            name: row.get(6)?,
        })
    }

    pub fn id(&self) -> DbId {
        self.id
    }

    pub fn grok_code(&self) -> &Option<String> {
        &self.grok_code
    }

    pub fn server(&self) -> &Option<String> {
        &self.server
    }

    pub fn giu_code(&self) -> &Option<String> {
        &self.giu_code
    }

    pub fn name(&self) -> &Option<String> {
        &self.name
    }

    pub fn project(&self) -> &Option<String> {
        &self.project
    }

    pub fn language(&self) -> &Option<String> {
        &self.language
    }
}

impl FromRow for Site {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            id: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            grok_code: row.get(1).unwrap(),
            server: row.get(2).unwrap(),
            giu_code: row.get(3).unwrap(),
            project: row.get(4).unwrap(),
            language: row.get(5).unwrap(),
            name: Baglama2::value2opt_string(
                row.as_ref(6)
                    .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            ),
        })
    }
}
