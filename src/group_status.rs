//! The tool DB's `group_status` table: one row per group and month, with its
//! processing status and total views. The web API lists a group-month once
//! its status is [`GroupStatus::Complete`]. (`storage` is always `'gzb'`, the
//! column default.)

use crate::db::Db;
use crate::group_id::GroupId;
use crate::YearMonth;
use anyhow::{anyhow, Result};
use log::warn;
use mysql_async::prelude::*;
use std::str::FromStr;

/// Where a group-month is in `gzb_month`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStatus {
    /// Phase 1 started: listing the group's files and their usage.
    Listing,
    /// Phase 1 done: the page list is in the work directory.
    Scanned,
    /// The gzb file is written; the web API shows the month.
    Complete,
    Failed,
}

impl GroupStatus {
    /// The value in `group_status.status`. The PHP API and the `overview`
    /// view match these strings, so they must not change.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Listing => "GENERATING PAGE LIST",
            Self::Scanned => "SCANNED",
            Self::Complete => "VIEW DATA COMPLETE",
            Self::Failed => "FAILED",
        }
    }
}

impl FromStr for GroupStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        [Self::Listing, Self::Scanned, Self::Complete, Self::Failed]
            .into_iter()
            .find(|status| status.as_str() == s)
            .ok_or_else(|| anyhow!("unknown group status '{s}'"))
    }
}

/// Upsert a group's row for the month.
pub async fn set(
    db: &Db,
    group_id: GroupId,
    ym: &YearMonth,
    status: GroupStatus,
    total_views: Option<u64>,
) -> Result<()> {
    let sql = "INSERT INTO `group_status` (group_id,year,month,status,total_views)
        VALUES (?,?,?,?,?)
        ON DUPLICATE KEY UPDATE status=VALUES(status),total_views=VALUES(total_views)";
    db.get_tooldb_conn()
        .await?
        .exec_drop(
            sql,
            (
                group_id.get(),
                ym.year(),
                ym.month(),
                status.as_str(),
                total_views,
            ),
        )
        .await?;
    Ok(())
}

/// `(status, rows)` for the month.
pub async fn counts(db: &Db, ym: &YearMonth) -> Result<Vec<(String, u64)>> {
    let rows = db
        .get_tooldb_conn()
        .await?
        .exec(
            "SELECT status,COUNT(*) FROM group_status WHERE year=? AND month=? GROUP BY status",
            (ym.year(), ym.month()),
        )
        .await?;
    Ok(rows)
}

/// Every group as `(id, is_active, status for the month if any)`.
///
/// A status this code does not know is logged and treated as no status, so
/// `gzb_month` lists the group again.
pub async fn groups_for_month(
    db: &Db,
    ym: &YearMonth,
) -> Result<Vec<(GroupId, bool, Option<GroupStatus>)>> {
    let rows: Vec<(usize, u8, Option<String>)> = db
        .get_tooldb_conn()
        .await?
        .exec(
            "SELECT g.id,g.is_active,gs.status FROM `groups` g
             LEFT JOIN group_status gs ON gs.group_id=g.id AND gs.year=? AND gs.month=?",
            (ym.year(), ym.month()),
        )
        .await?;
    rows.into_iter()
        .map(|(id, is_active, status)| {
            let id = GroupId::try_from(id)?;
            let status = status.and_then(|s| match s.parse() {
                Ok(status) => Some(status),
                Err(e) => {
                    warn!("group {id}, {ym}: {e}; listing it again");
                    None
                }
            });
            Ok((id, is_active == 1, status))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_strings_round_trip() {
        for status in [
            GroupStatus::Listing,
            GroupStatus::Scanned,
            GroupStatus::Complete,
            GroupStatus::Failed,
        ] {
            assert_eq!(status.as_str().parse::<GroupStatus>().unwrap(), status);
        }
        // The value the PHP API lists by.
        assert_eq!(GroupStatus::Complete.as_str(), "VIEW DATA COMPLETE");
        assert!("view data complete".parse::<GroupStatus>().is_err());
        assert!("".parse::<GroupStatus>().is_err());
    }
}
