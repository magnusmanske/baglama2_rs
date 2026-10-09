use crate::wiki::Dbname;
use anyhow::Result;
use log::warn;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct Site {
    server: Option<String>,
    /// `None` if the stored value is not a valid database name.
    giu_code: Option<Dbname>,
}

impl Site {
    pub fn server(&self) -> &Option<String> {
        &self.server
    }

    pub fn giu_code(&self) -> Option<&Dbname> {
        self.giu_code.as_ref()
    }
}

impl FromRow for Site {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            server: row
                .get_opt(0)
                .and_then(Result::ok)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            giu_code: row
                .get_opt::<Option<String>, _>(1)
                .and_then(Result::ok)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?
                .and_then(|giu| match Dbname::parse(&giu) {
                    Ok(giu) => Some(giu),
                    Err(e) => {
                        warn!("sites: {e}; ignoring that wiki");
                        None
                    }
                }),
        })
    }
}
