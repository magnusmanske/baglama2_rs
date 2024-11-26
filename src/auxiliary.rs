use std::{fmt::Display, str::Utf8Error};
use rusqlite::Result;
use mysql_async::prelude::*;
use crate::baglama2::*;



#[derive(Debug)]
pub enum Error {
    MySql(mysql_async::Error),
    Sqlite(rusqlite::Error),
    StdIo(std::io::Error),
    NoGroupForStatus,
    Sql(String),
    Date(String),
    Utf8Error(String),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<mysql_async::Error> for Error {
    fn from(err: mysql_async::Error) -> Self {
        Error::MySql(err)
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::Utf8Error(err.to_string())
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Error::Sqlite(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::StdIo(err)
    }
}


#[derive(Debug, Clone)]
pub struct ViewCount {
    pub view_id: usize,
    pub title: String,
    pub namespace_id: i32,
    pub grok_code: Option<String>,
    pub server: Option<String>,
    pub done: i8,
    pub site_id: usize,
}

impl ViewCount {
    pub fn from_row(row: &rusqlite::Row) -> Result<Self> {
        Ok(Self {
            view_id: row.get(0)?,
            title: row.get(1)?,
            namespace_id: row.get(2)?,
            grok_code: row.get(3)?,
            server: row.get(4)?,
            done: row.get(5)?,
            site_id: row.get(6)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct YearMonth {
    year: i32,
    month: u32,
}

impl YearMonth {
    pub fn new(year: i32, month: u32) -> Self {
        if month ==0 || month > 12 {
            panic!("Bad month: {month}");
        }
        if year < 2000 || year > 2030 {
            panic!("Bad year: {year}");
        }
        Self{year,month}
    }

    pub fn year(&self) -> i32 {
        self.year
    }

    pub fn month(&self) -> u32 {
        self.month
    }

    pub fn make_production_directory(&self, baglama: &Baglama2) -> Result<String,Error> {
        let subdir = chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .ok_or(Error::Date(format!("{}/{}",self.year,self.month)))?
            .format("%Y%m").to_string();
        let dir = format!("{}/{}",baglama.sqlite_data_root_path(),&subdir);
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    // TESTED
    pub fn first_day(&self) -> Result<String,Error> {
        let first_day =  chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .ok_or(Error::Date(format!("{}/{}",self.year,self.month)))?;
        Ok(first_day.format("%Y%m%d").to_string())
    }

    // TESTED
    pub fn last_day(&self) -> Result<String,Error> {
        let last_day =  chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .ok_or(Error::Date(format!("{}/{}",self.year,self.month)))? 
            + chronoutil::RelativeDuration::months(1) 
            + chronoutil::RelativeDuration::days(-1);
        Ok(last_day.format("%Y%m%d").to_string())
    }
}

impl std::fmt::Display for YearMonth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tmp = chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1).unwrap().format("%Y-%m").to_string();
        f.write_str(&tmp)
    }
}

#[derive(Debug, Clone)]
pub struct BaglamaGroup {
    id: usize,
}

impl BaglamaGroup {
    pub fn new(id: usize) -> Self {
        if id==0 {
            panic!("Bad group ID: {id}");
        }
        Self {
            id,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupStatus {
    pub id: usize,
    pub group_id: usize,
    pub year: i32,
    pub month: u32,
    pub status: String,
    pub total_views: Option<usize>,
    pub file: Option<String>,
    pub sqlite3: Option<String>,
}

impl FromRow for RowGroupStatus {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError> where Self:Sized {
        Ok(Self {
            id: row.get(0).unwrap(), 
            group_id: row.get(1).unwrap(), 
            year: row.get(2).unwrap(), 
            month: row.get(3).unwrap(), 
            status: row.get(4).unwrap(), 
            total_views: row.get(5).unwrap(), 
            file: row.get(6).unwrap(), 
            sqlite3: row.get(7).unwrap(), 
        })
    }
}


#[derive(Debug, Clone)]
pub struct Site {
    id: usize,
    pub grok_code:Option<String>,
    pub server:Option<String>,
    pub giu_code:Option<String>,
    project:Option<String>,
    language:Option<String>,
    pub name:Option<String>,
}

impl Site {
    pub fn from_sqlite_row(row: &rusqlite::Row) -> Result<Self> {
        Ok(Self { 
            id: row.get(0)?, 
            grok_code: row.get(1)?, 
            server: row.get(2)?, 
            giu_code: row.get(3)?, 
            project: row.get(4)?, 
            language: row.get(5)?, 
            name: row.get(6)?,
        })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn project(&self) -> &Option<String> {
        &self.project
    }

    pub fn language(&self) -> &Option<String> {
        &self.language
    }
}


impl FromRow for Site {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError> where Self:Sized {
        Ok(Self {
            id: row.get(0).unwrap(), 
            grok_code: row.get(1).unwrap(), 
            server: row.get(2).unwrap(), 
            giu_code: row.get(3).unwrap(), 
            project: row.get(4).unwrap(), 
            language: row.get(5).unwrap(), 
            name: Baglama2::value2opt_string(row.as_ref(6).unwrap()),
        })
    }
}


#[derive(Debug, Clone)]
pub struct RowGroup {
    pub id: usize,
    pub category: String,
    pub depth: usize,
    pub added_by: String,
    pub just_added: u8,
}

impl FromRow for RowGroup {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError> where Self:Sized {
        Ok(Self {
            id: row.get(0).unwrap(), 
            category: Baglama2::value2opt_string(row.as_ref(1).unwrap()).unwrap(),
            depth: row.get(2).unwrap(), 
            added_by: Baglama2::value2opt_string(row.as_ref(3).unwrap()).unwrap(),
            just_added: row.get(4).unwrap(), 
        })
    }
}

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
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError> where Self:Sized {
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_first_last_day() {
        let ym = YearMonth::new(2020,2);
        assert_eq!(ym.first_day().unwrap().as_str(),"20200201");
        assert_eq!(ym.last_day().unwrap().as_str(),"20200229");
    }
}