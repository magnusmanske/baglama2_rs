use crate::{db_mysql::DbMySql, YearMonth};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct MonthViews {
    ym: YearMonth,
    table: String,
    table_exists: bool,
}

impl MonthViews {
    pub fn new(ym: YearMonth) -> Self {
        Self {
            ym,
            table: format!("month_views_{}", ym.to_string().replace('-', "_")),
            table_exists: false,
        }
    }

    pub async fn create_table_if_not_exists(&mut self, db: &DbMySql) -> Result<()> {
        if self.table_exists {
            return Ok(());
        }
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} {{
        	`page_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
         	`views` int(11) unsigned DEFAULT NULL,
          	PRIMARY KEY (`page_id`)
        }}",
            self.table
        );
        println!("{sql}");
        db.execute(&sql).await?;
        self.table_exists = true;
        Ok(())
    }
}
