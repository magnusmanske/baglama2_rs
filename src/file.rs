use crate::{baglama2::Baglama2, DbId};
use mysql_async::prelude::*;

pub struct File {
    pub id: Option<DbId>,
    pub name: String,
}

impl File {
    pub fn new(id: Option<DbId>, name: String) -> Self {
        File { id, name }
    }

    pub fn new_no_id(name: &str) -> Self {
        File {
            id: None,
            name: name.to_owned(),
        }
    }
}

impl FromRow for File {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let title = Baglama2::value2opt_string(
            row.as_ref(1)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
        )
        .ok_or_else(|| mysql_async::FromRowError(row.clone()))?;

        Ok(Self::new(
            row.get("id")
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            title,
        ))
    }
}
