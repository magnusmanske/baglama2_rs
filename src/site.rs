use crate::DbId;
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct Site {
    id: DbId,
    server: Option<String>,
    giu_code: Option<String>,
}

impl Site {
    pub fn id(&self) -> DbId {
        self.id
    }

    pub fn server(&self) -> &Option<String> {
        &self.server
    }

    pub fn giu_code(&self) -> &Option<String> {
        &self.giu_code
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
            server: row.get(1).unwrap(),
            giu_code: row.get(2).unwrap(),
        })
    }
}
