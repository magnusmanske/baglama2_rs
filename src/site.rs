use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct Site {
    server: Option<String>,
    giu_code: Option<String>,
}

impl Site {
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
            server: row.get(0).unwrap(),
            giu_code: row.get(1).unwrap(),
        })
    }
}
