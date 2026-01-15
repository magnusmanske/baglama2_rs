use crate::DbId;
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageType {
    File,
    Mysql,
    Sqlite3,
}

impl TryFrom<&str> for StorageType {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "file" => Ok(StorageType::File),
            "mysql" => Ok(StorageType::Mysql),
            "sqlite3" => Ok(StorageType::Sqlite3),
            _ => Err("Invalid storage type!"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupStatus {
    pub id: DbId,
    pub group_id: DbId,
    pub year: i32,
    pub month: u32,
    pub status: String,
    pub total_views: Option<isize>,
    pub file: Option<String>,
    pub sqlite3: Option<String>,
    pub storage: StorageType,
}

impl FromRow for RowGroupStatus {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let storage: String = row
            .get(8)
            .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?;
        Ok(Self {
            id: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            group_id: row
                .get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            year: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            month: row
                .get(3)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            status: row
                .get(4)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            total_views: row.get(5).unwrap(),
            file: row.get(6).unwrap(),
            sqlite3: row.get(7).unwrap(),
            storage: storage
                .as_str()
                .try_into()
                .map_err(|_| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}

impl RowGroupStatus {
    pub fn sql_all() -> String {
        "id,group_id,year,month,status,total_views,file,sqlite3,storage".to_string()
    }
}
