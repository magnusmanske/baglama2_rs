use crate::{baglama2::*, DbId};
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct RowGroup {
    id: DbId,
    category: String,
    depth: isize,
    is_user_name: u8,
}

impl RowGroup {
    /// Returns the SQL base to be used in `FromRow::from_row_opt`.
    pub fn sql_select() -> String {
        "SELECT id,FROM_BASE64(TO_BASE64(category)),depth,is_user_name FROM `groups`".to_string()
    }
    pub fn id(&self) -> DbId {
        self.id
    }

    pub fn category(&self) -> &String {
        &self.category
    }

    pub fn depth(&self) -> isize {
        self.depth
    }

    pub fn is_user_name(&self) -> bool {
        self.is_user_name == 1
    }
}

impl FromRow for RowGroup {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            id: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            category: Baglama2::value2opt_string(
                row.as_ref(1)
                    .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?
            .trim()
            .to_string(),
            depth: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            is_user_name: row
                .get(3)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}
