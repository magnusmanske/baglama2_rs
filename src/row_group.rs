use crate::baglama2::*;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct RowGroup {
    id: usize,
    category: String,
    depth: usize,
    added_by: String,
    just_added: u8,
}

impl RowGroup {
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
