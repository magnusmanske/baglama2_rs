use crate::baglama2::*;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct RowGroup {
    id: usize,
    category: String,
    depth: usize,
    added_by: String,
    just_added: u8,
    is_active: u8,
    is_user_name: u8,
}

impl RowGroup {
    /// Returns the SQL base to be used in `FromRow::from_row_opt`.
    pub fn sql_select() -> String {
        "SELECT id,FROM_BASE64(TO_BASE64(category)),depth,FROM_BASE64(TO_BASE64(added_by)),just_added,is_active,is_user_name FROM `groups`".to_string()
    }
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn category(&self) -> &String {
        &self.category
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn added_by(&self) -> &String {
        &self.added_by
    }

    pub fn just_added(&self) -> u8 {
        self.just_added
    }

    pub fn is_active(&self) -> u8 {
        self.is_active
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
            added_by: Baglama2::value2opt_string(
                row.as_ref(3)
                    .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            just_added: row
                .get(4)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            is_active: row
                .get(5)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            is_user_name: row
                .get(6)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}
