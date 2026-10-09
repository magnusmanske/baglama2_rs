use crate::db::{sql_placeholders, value2opt_string, Db};
use crate::wiki::Dbname;
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct GlobalImageLinks {
    pub wiki: Dbname,
    pub page_namespace_id: i32,
    /// Local namespace name, e.g. `Kategorie`; empty for the main namespace.
    pub page_namespace: String,
    pub page_title: String,
    pub to: String,
}

impl GlobalImageLinks {
    /// The title as it appears in the pageview dump: namespace-prefixed,
    /// underscored.
    pub fn dump_title(&self) -> String {
        let title = if self.page_namespace.is_empty() {
            self.page_title.clone()
        } else {
            format!("{}:{}", self.page_namespace, self.page_title)
        };
        title.replace(' ', "_")
    }

    /// Calls `f` for each usage of `files` as the rows arrive, so a batch
    /// with a widely used file (millions of usages) is never held in memory.
    /// On a retried query `f` can see a usage twice; the page list tolerates
    /// duplicate rows.
    pub async fn for_each<F>(files: &[String], db: &Db, f: F) -> Result<()>
    where
        F: FnMut(GlobalImageLinks) -> Result<()>,
    {
        if files.is_empty() {
            return Ok(());
        }
        let placeholders = sql_placeholders(files.len());
        let sql = format!("SELECT gil_wiki,gil_page_namespace_id,FROM_BASE64(TO_BASE64(gil_page_namespace)),FROM_BASE64(TO_BASE64(gil_page_title)),FROM_BASE64(TO_BASE64(gil_to)) FROM `globalimagelinks` WHERE `gil_to` IN ({})",placeholders);

        // `globalimagelinks` moved to the Commons links cluster (`x4`);
        // `query_commons_each` sends this to the pool that can read it.
        db.query_commons_each(&["globalimagelinks"], &sql, files.to_vec(), f)
            .await
    }

    #[cfg(test)]
    pub async fn load(files: &[String], db: &Db) -> Result<Vec<GlobalImageLinks>> {
        let mut usages = vec![];
        Self::for_each(files, db, |gil| {
            usages.push(gil);
            Ok(())
        })
        .await?;
        Ok(usages)
    }
}

impl FromRow for GlobalImageLinks {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let ret = Self {
            wiki: row
                .get::<String, _>(0)
                .and_then(|wiki| Dbname::parse(&wiki).ok())
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page_namespace_id: row
                .get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page_namespace: row.as_ref(2).and_then(value2opt_string).unwrap_or_default(),
            page_title: value2opt_string(
                row.as_ref(3)
                    .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            to: value2opt_string(
                row.as_ref(4)
                    .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
        };
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_load() {
        let db = Db::new(&crate::config::Config::load().unwrap()).unwrap();
        let files = vec!["Albert_Einstein_Head.jpg".to_string()];
        let gils = GlobalImageLinks::load(&files, &db).await.unwrap();
        assert!(gils.len() > 10);
        assert!(gils.iter().all(|gil| gil.to == files[0]));
        // Namespace columns line up: names only outside the main namespace.
        assert!(gils
            .iter()
            .all(|gil| (gil.page_namespace_id == 0) == gil.page_namespace.is_empty()));
        assert!(gils.iter().any(|gil| gil.page_namespace_id == 0));
        assert!(gils.iter().any(|gil| gil.page_namespace_id != 0));
    }
}
