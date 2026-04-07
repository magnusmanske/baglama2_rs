use crate::db_mysql2::DbMySql2;
use anyhow::Result;
use baglama2::*;
use chrono::{DateTime, Datelike, Months, Utc};
use group_date::*;
use log::{info, LevelFilter};
pub use site::Site;
use std::env;
use std::num::NonZero;
use std::sync::Arc;
use tokio::sync::Semaphore;
pub use view_count::ViewCount;
pub use year_month::YearMonth;

pub type DbId = usize;

pub mod baglama2;
pub mod db_mysql2;
pub mod db_sqlite;
pub mod db_trait;
pub mod file;
pub mod global_image_links;
pub mod group_date;
pub mod month_views;
pub mod page;
pub mod pageviews;
pub mod row_group;
pub mod row_group_status;
pub mod site;
pub mod view_count;
pub mod year_month;

pub type GroupId = NonZero<DbId>;

fn month(month: Option<&String>) -> u32 {
    match month.map(|s| s.as_str()) {
        Some("lm") => {
            let last: DateTime<Utc> = Utc::now()
                .checked_sub_months(Months::new(1))
                .unwrap_or_else(|| panic!("Bad month: could not subtract 1 month from current date (input: {month:?})"));
            last.month()
        }
        Some(s) => s
            .parse::<u32>()
            .unwrap_or_else(|_| panic!("Month: number expected, not '{s}'")),
        None => panic!("Month expected but missing"),
    }
}

fn year(year: Option<&String>) -> i32 {
    match year.map(|s| s.as_str()) {
        Some("lm") => {
            let last: DateTime<Utc> = Utc::now()
                .checked_sub_months(Months::new(1))
                .unwrap_or_else(|| {
                    panic!(
                        "Bad year: could not subtract 1 month from current date (input: {year:?})"
                    )
                });
            last.year()
        }
        Some(s) => s
            .parse::<i32>()
            .unwrap_or_else(|_| panic!("Year: number expected, not '{s}'")),
        None => panic!("Year expected but missing"),
    }
}

async fn process_all_groups(
    year: i32,
    month: u32,
    baglama: Arc<Baglama2>,
    requires_previous_date: bool,
) -> Result<()> {
    baglama.clear_incomplete_group_status(year, month).await?;
    let max_concurrent = baglama.config()["max_concurrent_jobs"]
        .as_u64()
        .unwrap_or(4) as usize;

    // A Semaphore is the idiomatic async primitive for bounding concurrency.
    // It replaces the previous std::sync::Mutex<u64> busy-loop, which held a
    // blocking lock across .await points and polled in a spin loop.
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    let mut join_set = tokio::task::JoinSet::new();

    loop {
        // Drain any completed tasks so the JoinSet doesn't grow unboundedly.
        while let Some(res) = join_set.try_join_next() {
            if let Err(e) = res {
                info!("Spawned task panicked: {:?}", e);
            }
        }

        let group_id_opt = baglama
            .get_next_group_id(year, month, requires_previous_date)
            .await;

        if let Some(group_id) = group_id_opt {
            // Acquire a permit *before* spawning so we never exceed the limit.
            // clone() gives the spawned task its own permit that releases on drop.
            let permit = Arc::clone(&semaphore)
                .acquire_owned()
                .await
                .expect("Semaphore closed unexpectedly");

            info!(
                "Now ~{} jobs running",
                max_concurrent - semaphore.available_permits()
            );

            let baglama = baglama.clone();
            let mut gd = GroupDate::new(
                group_id.try_into()?,
                YearMonth::new(year, month)
                    .unwrap_or_else(|_| panic!("Bad year/month: {year}/{month}")),
                baglama.clone(),
            );
            let _ = gd.set_group_status("GENERATING PAGE LIST", 0, "").await;
            join_set.spawn(async move {
                match gd.create_sqlite().await {
                    Ok(_) => {}
                    Err(err) => {
                        let _ = gd.set_group_status("FAILED", 0, "").await;
                        info!("{group_id} failed: {:?}", &err);
                    }
                }
                // Dropping the permit here releases the semaphore slot.
                drop(permit);
            });
        } else {
            // No more groups to schedule; wait for all in-flight tasks to finish.
            join_set.join_all().await;
            info!("Complete");
            break;
        }
    }
    Ok(())
}

async fn process_mysql2(ym: YearMonth, baglama: Arc<Baglama2>) -> Result<()> {
    let db = DbMySql2::new(ym, baglama.clone()).await?;
    db.ensure_table_exists().await?;
    db.start_missing_groups().await?;
    db.add_pages().await?;
    Ok(())
}

async fn process_mysql2_views(ym: YearMonth, baglama: Arc<Baglama2>) -> Result<()> {
    let db = DbMySql2::new(ym, baglama.clone()).await?;
    db.ensure_table_exists().await?;
    db.load_missing_views().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    log::set_max_level(LevelFilter::Trace);
    let argv: Vec<String> = env::args_os()
        .map(|s| s.into_string().expect("Bad argv"))
        .collect();
    let baglama = Arc::new(Baglama2::new().await?);
    baglama.deactivate_nonexistent_categories().await?;
    match argv.get(1).map(|s| s.as_str()) {
        Some("mysql2") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            baglama.update_sites().await?;
            process_mysql2(
                YearMonth::new(year, month).expect("bad year/month"),
                baglama.clone(),
            )
            .await?;
        }
        Some("mysql2_views") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            baglama.update_sites().await?;
            process_mysql2_views(
                YearMonth::new(year, month).expect("bad year/month"),
                baglama.clone(),
            )
            .await?;
        }
        Some("_run") => {
            let group_id = argv
                .get(2)
                .map(|s| s.parse::<DbId>().expect("bad group ID"))
                .expect("Group ID expected");
            let year = year(argv.get(3));
            let month = month(argv.get(4));
            let mut gd = GroupDate::new(
                group_id.try_into()?,
                YearMonth::new(year, month).expect("bad year/month"),
                baglama.clone(),
            );
            let _ = gd.set_group_status("GENERATING PAGE LIST", 0, "").await;
            gd.create_mysql2().await?;
            // gd.create_sqlite().await?;
        }
        Some("_next") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            if let Some(group_id) = baglama.get_next_group_id(year, month, false).await {
                GroupDate::new(
                    group_id.try_into()?,
                    YearMonth::new(year, month).expect("bad year/month"),
                    baglama.clone(),
                )
                .create_sqlite()
                .await?;
            } else {
                info!("No more groups for {year}/{month}");
            }
        }
        Some("_next_all_seq") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            if argv.get(4).is_none() {
                // Any third parameter will do this
                baglama.clear_incomplete_group_status(year, month).await?;
            }
            loop {
                if let Some(group_id) = baglama.get_next_group_id(year, month, false).await {
                    let mut gd = GroupDate::new(
                        group_id.try_into()?,
                        YearMonth::new(year, month).expect("bad year/month"),
                        baglama.clone(),
                    );
                    let _ = gd.set_group_status("GENERATING PAGE LIST", 0, "").await;
                    match gd.create_sqlite().await {
                        Ok(_) => {}
                        Err(err) => {
                            let _ = gd.set_group_status("FAILED", 0, "").await;
                            info!("{group_id} failed: {:?}", &err);
                        }
                    }
                } else {
                    info!("No more groups for {year}/{month}");
                    break;
                }
            }
        }
        Some("_next_all") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            process_all_groups(year, month, baglama.clone(), false).await?;
        }
        Some("_backfill") => {
            let mut year = year(argv.get(2));
            let mut month = month(argv.get(3));
            let current_year = chrono::Utc::now().year();
            let current_month = chrono::Utc::now().month();
            loop {
                info!("BACKFILLING {year}-{month}");
                process_all_groups(year, month, baglama.clone(), true).await?;
                month += 1;
                if month > 12 {
                    month = 1;
                    year += 1;
                }
                if year > current_year || (year == current_year && month >= current_month) {
                    break;
                }
            }
        }
        Some("_test") => {
            let current_month = chrono::Utc::now().month();
            info!("{current_month}");
        }
        other => panic!("Action required (not {:?}", other),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_month_numeric() {
        assert_eq!(month(Some(&"3".to_string())), 3);
        assert_eq!(month(Some(&"12".to_string())), 12);
        assert_eq!(month(Some(&"1".to_string())), 1);
    }

    #[test]
    #[should_panic(expected = "Month: number expected, not 'foo'")]
    fn test_month_bad_string_panics_with_value() {
        month(Some(&"foo".to_string()));
    }

    #[test]
    #[should_panic(expected = "Month expected but missing")]
    fn test_month_none_panics() {
        month(None);
    }

    #[test]
    fn test_year_numeric() {
        assert_eq!(year(Some(&"2023".to_string())), 2023);
        assert_eq!(year(Some(&"2000".to_string())), 2000);
    }

    #[test]
    #[should_panic(expected = "Year: number expected, not 'bar'")]
    fn test_year_bad_string_panics_with_value() {
        year(Some(&"bar".to_string()));
    }

    #[test]
    #[should_panic(expected = "Year expected but missing")]
    fn test_year_none_panics() {
        year(None);
    }
}
