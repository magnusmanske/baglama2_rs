//! The tool DB's `group_status` table: one row per group and month, with its
//! processing status and total views. The web API lists a group-month once
//! its status is [`STATUS_COMPLETE`]. (`storage` is always `'gzb'`, the
//! column default.)

use crate::db::Db;
use crate::YearMonth;
use anyhow::Result;
use mysql_async::prelude::*;

pub const STATUS_LISTING: &str = "GENERATING PAGE LIST";
pub const STATUS_SCANNED: &str = "SCANNED";
pub const STATUS_COMPLETE: &str = "VIEW DATA COMPLETE";
pub const STATUS_FAILED: &str = "FAILED";

/// Upsert a group's row for the month.
pub async fn set(
    db: &Db,
    group_id: usize,
    ym: &YearMonth,
    status: &str,
    total_views: Option<u64>,
) -> Result<()> {
    let sql = "INSERT INTO `group_status` (group_id,year,month,status,total_views)
        VALUES (?,?,?,?,?)
        ON DUPLICATE KEY UPDATE status=VALUES(status),total_views=VALUES(total_views)";
    db.get_tooldb_conn()
        .await?
        .exec_drop(sql, (group_id, ym.year(), ym.month(), status, total_views))
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
pub async fn groups_for_month(
    db: &Db,
    ym: &YearMonth,
) -> Result<Vec<(usize, bool, Option<String>)>> {
    let rows: Vec<(usize, u8, Option<String>)> = db
        .get_tooldb_conn()
        .await?
        .exec(
            "SELECT g.id,g.is_active,gs.status FROM `groups` g
             LEFT JOIN group_status gs ON gs.group_id=g.id AND gs.year=? AND gs.month=?",
            (ym.year(), ym.month()),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, is_active, status)| (id, is_active == 1, status))
        .collect())
}
