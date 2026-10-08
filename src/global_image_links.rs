use crate::Baglama2;
use anyhow::Result;
use mysql_async::prelude::*;

#[derive(Debug, Clone)]
pub struct GlobalImageLinks {
    pub wiki: String,
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

    pub async fn load(files: &[String], baglama: &Baglama2) -> Result<Vec<GlobalImageLinks>> {
        if files.is_empty() {
            return Ok(vec![]);
        }
        let placeholders = Baglama2::sql_placeholders(files.len());
        let sql = format!("SELECT gil_wiki,gil_page_namespace_id,FROM_BASE64(TO_BASE64(gil_page_namespace)),FROM_BASE64(TO_BASE64(gil_page_title)),FROM_BASE64(TO_BASE64(gil_to)) FROM `globalimagelinks` WHERE `gil_to` IN ({})",placeholders);

        // `globalimagelinks` moved to the Commons links cluster (`x4`);
        // `query_commons` sends this to the pool that can read it.
        baglama
            .query_commons(&["globalimagelinks"], &sql, files.to_vec())
            .await
    }
}

impl FromRow for GlobalImageLinks {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let ret = Self {
            wiki: row
                .get(0)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page_namespace_id: row
                .get(1)
                .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            page_namespace: row
                .as_ref(2)
                .and_then(Baglama2::value2opt_string)
                .unwrap_or_default(),
            page_title: Baglama2::value2opt_string(
                row.as_ref(3)
                    .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.clone()))?,
            to: Baglama2::value2opt_string(
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
    async fn test_load() {
        let baglama = Baglama2::new().await.unwrap();
        let files = vec!["Albert_Einstein_Head.jpg".to_string()];
        let gils = GlobalImageLinks::load(&files, &baglama).await.unwrap();
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
