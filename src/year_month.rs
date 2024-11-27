use crate::Baglama2;
use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct YearMonth {
    year: i32,
    month: u32,
}

impl YearMonth {
    pub fn new(year: i32, month: u32) -> Self {
        if month == 0 || month > 12 {
            panic!("Bad month: {month}");
        }
        if !(2000..=2030).contains(&year) {
            panic!("Bad year: {year}");
        }
        Self { year, month }
    }

    pub fn year(&self) -> i32 {
        self.year
    }

    pub fn month(&self) -> u32 {
        self.month
    }

    pub fn make_production_directory(&self, baglama: &Baglama2) -> Result<String> {
        let subdir = chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .ok_or(anyhow!(format!("{}/{}", self.year, self.month)))?
            .format("%Y%m")
            .to_string();
        let dir = format!("{}/{}", baglama.sqlite_data_root_path(), &subdir);
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    // TESTED
    pub fn first_day(&self) -> Result<String> {
        let first_day = chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .ok_or(anyhow!(format!("{}/{}", self.year, self.month)))?;
        Ok(first_day.format("%Y%m%d").to_string())
    }

    // TESTED
    pub fn last_day(&self) -> Result<String> {
        let last_day = chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .ok_or(anyhow!(format!("{}/{}", self.year, self.month)))?
            + chronoutil::RelativeDuration::months(1)
            + chronoutil::RelativeDuration::days(-1);
        Ok(last_day.format("%Y%m%d").to_string())
    }
}

impl std::fmt::Display for YearMonth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tmp = chrono::NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .unwrap()
            .format("%Y-%m")
            .to_string();
        f.write_str(&tmp)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_first_last_day() {
        let ym = YearMonth::new(2020, 2);
        assert_eq!(ym.first_day().unwrap().as_str(), "20200201");
        assert_eq!(ym.last_day().unwrap().as_str(), "20200229");
    }
}
