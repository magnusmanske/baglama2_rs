use crate::Baglama2;
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct GlobalImageLinks {
    pub wiki: String,
    pub page: usize,
    pub page_namespace_id: i32,
    //pub page_namespace: String,
    pub page_title: String,
    pub to: String,
}

impl FromRow for GlobalImageLinks {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self {
            wiki: row.get(0).unwrap(),
            page: row.get(1).unwrap(),
            page_namespace_id: row.get(2).unwrap(),
            //page_namespace: row.get(3).unwrap(),
            page_title: Baglama2::value2opt_string(row.as_ref(4).unwrap()).unwrap(),
            to: Baglama2::value2opt_string(row.as_ref(5).unwrap()).unwrap(),
        })
    }
}
