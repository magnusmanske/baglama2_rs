use crate::baglama2::*;
use mysql_async::prelude::*;

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
