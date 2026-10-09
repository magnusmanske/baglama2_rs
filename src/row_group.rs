use crate::category::{effective_depth, CategoryTitle};
use crate::db::{value2opt_string, Db};
use crate::GroupId;
use anyhow::{anyhow, Result};
use mysql_async::{from_row_opt, prelude::*, FromRowError, Row};

/// What a group tracks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupSource {
    /// Files in a category tree, `depth` levels of subcategories deep as
    /// stored; the walk is capped at [`crate::category::MAX_DEPTH`], see
    /// [`crate::category::category_tree`].
    Category { title: CategoryTitle, depth: isize },
    /// Files uploaded by a user.
    Uploader(String),
}

/// A row of the tool DB's `groups` table.
#[derive(Debug, Clone)]
pub struct RowGroup {
    id: GroupId,
    source: GroupSource,
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
            .map_and_drop(from_row_opt::<Self>)
            .await?;
        match groups.into_iter().next() {
            None => Ok(None),
            Some(Ok(group)) => Ok(Some(group)),
            Some(Err(e)) => Err(anyhow!("group {group_id}: unusable row: {e}")),
        }
    }

    pub fn id(&self) -> GroupId {
        self.id
    }

    pub fn source(&self) -> &GroupSource {
        &self.source
    }

    /// What the group tracks, for people: `Category:NASA (depth 5)`.
    pub fn label(&self) -> String {
        match &self.source {
            GroupSource::Category { title, depth } => {
                let walked = effective_depth(*depth);
                if walked == *depth {
                    format!("Category:{title} (depth {depth})")
                } else {
                    format!("Category:{title} (depth {depth}, capped at {walked})")
                }
            }
            GroupSource::Uploader(name) => format!("files uploaded by User:{name}"),
        }
    }
}

impl FromRow for RowGroup {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        let bad = || FromRowError(row.clone());
        let id: usize = row.get(0).ok_or_else(bad)?;
        let name = value2opt_string(row.as_ref(1).ok_or_else(bad)?).ok_or_else(bad)?;
        let depth: isize = row.get(2).ok_or_else(bad)?;
        let is_user_name: u8 = row.get(3).ok_or_else(bad)?;
        let source = if is_user_name == 1 {
            let name = name.trim();
            if name.is_empty() {
                return Err(bad());
            }
            GroupSource::Uploader(name.to_string())
        } else {
            GroupSource::Category {
                title: CategoryTitle::parse(&name).map_err(|_| bad())?,
                depth,
            }
        };
        Ok(Self {
            id: GroupId::try_from(id).map_err(|_| bad())?,
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    async fn load(id: usize) -> RowGroup {
        let db = Db::new(&Config::load().unwrap()).unwrap();
        RowGroup::load(&db, id.try_into().unwrap())
            .await
            .unwrap()
            .unwrap()
    }

    fn category(group: &RowGroup) -> String {
        match group.source() {
            GroupSource::Category { title, .. } => title.to_string(),
            GroupSource::Uploader(_) => panic!("not a category group"),
        }
    }

    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_load() {
        let group = load(1255).await;
        assert_eq!(
            category(&group),
            "Images from Archives of Ontario – RG 14-100 Official Road Maps of Ontario"
        );
    }

    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_load_utf8() {
        let group = load(292).await;
        assert_eq!(
            category(&group),
            "Files of Museum für Kunst und Gewerbe Hamburg uploaded by RKBot"
        );
    }

    /// Every stored group parses, and parsing changes no active category.
    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_all_groups_parse() {
        let db = Db::new(&Config::load().unwrap()).unwrap();
        let mut conn = db.get_tooldb_conn().await.unwrap();
        let raw: std::collections::HashMap<usize, (u8, String)> = conn
            .query::<(usize, u8, String), _>("SELECT id,is_active,category FROM `groups`")
            .await
            .unwrap()
            .into_iter()
            .map(|(id, is_active, category)| (id, (is_active, category)))
            .collect();
        let groups = conn
            .exec_iter(RowGroup::sql_select(), ())
            .await
            .unwrap()
            .map_and_drop(from_row_opt::<RowGroup>)
            .await
            .unwrap();
        assert_eq!(groups.len(), raw.len());
        let mut changed = vec![];
        for group in groups {
            let group = group.unwrap();
            let (is_active, stored) = &raw[&group.id().get()];
            if let GroupSource::Category { title, .. } = group.source() {
                if title.to_string() != *stored {
                    changed.push((group.id(), *is_active, stored.clone()));
                }
            }
        }
        // Only inactive groups may differ (908, "Image  Files", today).
        assert!(
            changed.iter().all(|(_, is_active, _)| *is_active == 0),
            "{changed:?}"
        );
        eprintln!("normalized differently: {changed:?}");
    }
}
