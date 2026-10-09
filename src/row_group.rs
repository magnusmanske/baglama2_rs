use crate::db::{value2opt_string, Db};
use crate::{DbId, GroupId};
use anyhow::Result;
use mysql_async::{from_row, prelude::*};

#[derive(Debug, Clone)]
pub struct RowGroup {
    id: DbId,
    category: String,
    depth: isize,
    is_user_name: u8,
}

impl RowGroup {
    /// Returns the SQL base to be used in `FromRow::from_row_opt`.
    pub fn sql_select() -> String {
        "SELECT id,FROM_BASE64(TO_BASE64(category)),depth,is_user_name FROM `groups`".to_string()
    }
    /// The group with ID `group_id`, if there is one.
    pub async fn load(db: &Db, group_id: GroupId) -> Result<Option<Self>> {
        let sql = format!("{} WHERE id={group_id}", Self::sql_select());
        let groups = db
            .get_tooldb_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<Self>)
            .await?;
        Ok(groups.into_iter().next())
    }

    pub fn id(&self) -> DbId {
        self.id
    }

    pub fn category(&self) -> &String {
        &self.category
    }

    pub fn depth(&self) -> isize {
        self.depth
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
            category: value2opt_string(
                row.as_ref(1)
                    .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            )
            .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?
            .trim()
            .to_string(),
            depth: row
                .get(2)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
            is_user_name: row
                .get(3)
                .ok_or_else(|| mysql_async::FromRowError(row.to_owned()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[tokio::test]
    async fn test_load() {
        let db = Db::new(&Config::load().unwrap()).unwrap();
        let group = RowGroup::load(&db, 1255.try_into().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            group.category(),
            "Images from Archives of Ontario – RG 14-100 Official Road Maps of Ontario"
        );
    }

    #[tokio::test]
    async fn test_load_utf8() {
        let db = Db::new(&Config::load().unwrap()).unwrap();
        let group = RowGroup::load(&db, 292.try_into().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            group.category(),
            "Files of Museum für Kunst und Gewerbe Hamburg uploaded by RKBot"
        );
    }
}
