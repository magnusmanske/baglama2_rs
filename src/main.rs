use anyhow::Result;
use baglama2::*;
use chrono::{DateTime, Datelike, Months, Utc};
use group_date::*;
use log::{info, LevelFilter};
pub use site::Site;
use std::env;
use std::num::NonZero;
use std::sync::{Arc, Mutex};
pub use view_count::ViewCount;
pub use year_month::YearMonth;

/* TODO
2023-09
2023-10
2023-11
2023-12
2024-01
2024-02
2024-03
2024-04
2024-05
2024-06
2024-07
2024-08
2024-09
*/

pub mod baglama2;
pub mod db_mysql;
pub mod db_sqlite;
pub mod db_trait;
pub mod global_image_links;
pub mod group_date;
pub mod row_group;
pub mod row_group_status;
pub mod site;
pub mod view_count;
pub mod year_month;

pub type GroupId = NonZero<usize>;

/*
ssh magnus@tools-login.wmflabs.org -L 3307:commonswiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:oxztsldqokc.svc.trove.eqiad1.wikimedia.cloud:3306 -N &
#ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &

git pull && ./build.sh && toolforge-jobs delete manual && rm ~/manual.* && \
toolforge-jobs run --image tf-php74 --mem 1500Mi --command '/data/project/glamtools/baglama2_rs/run_all.sh lm lm' monthly
*/

fn month(month: Option<&String>) -> u32 {
    match month.map(|s| s.as_str()) {
        Some("lm") => {
            let last: DateTime<Utc> = Utc::now()
                .checked_sub_months(Months::new(1))
                .expect("Bad month {month}");
            last.month()
        }
        Some(s) => s.parse::<u32>().expect("Month: number expected, not {s}"),
        None => panic!("Month expected but missing"),
    }
}

fn year(year: Option<&String>) -> i32 {
    match year.map(|s| s.as_str()) {
        Some("lm") => {
            let last: DateTime<Utc> = Utc::now()
                .checked_sub_months(Months::new(1))
                .expect("Bad year {year}");
            last.year()
        }
        Some(s) => s.parse::<i32>().expect("Year: number expected, not {s}"),
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
        .unwrap_or(4);
    let concurrent: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    loop {
        if *concurrent
            .lock()
            .expect("main: concurrent lock poisoned [1]")
            >= max_concurrent
        {
            baglama.hold_on().await;
            continue;
        }
        let group_id_opt = baglama
            .get_next_group_id(year, month, requires_previous_date)
            .await;
        if let Some(group_id) = group_id_opt {
            let concurrent = concurrent.clone();
            let baglama = baglama.clone();
            *concurrent
                .lock()
                .expect("main: concurrent lock poisoned [2]") += 1;
            info!(
                "Now {} jobs running",
                concurrent
                    .lock()
                    .expect("main: concurrent lock poisoned [3]")
            );
            let mut gd = GroupDate::new(
                group_id.try_into()?,
                YearMonth::new(year, month).expect("Bad year/month {year}/{month}"),
                baglama.clone(),
            );
            let _ = gd.set_group_status("GENERATING PAGE LIST", 0, "").await;
            tokio::spawn(async move {
                match gd.create_sqlite().await {
                    Ok(_) => {}
                    Err(err) => {
                        let _ = gd.set_group_status("FAILED", 0, "").await;
                        info!("{group_id} failed: {:?}", &err);
                    }
                }
                drop(gd);
                *concurrent
                    .lock()
                    .expect("main: concurrent lock poisoned [4]") -= 1;
            });
        } else {
            // Wait for threads to finish
            if *concurrent
                .lock()
                .expect("main: concurrent lock poisoned [5]")
                > 0
            {
                baglama.hold_on().await;
                continue;
            }
            info!("Complete");
            break;
        }
    }
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
        Some("run") => {
            let group_id = argv
                .get(2)
                .map(|s| s.parse::<usize>().expect("bad group ID"))
                .expect("Group ID expected");
            let year = year(argv.get(3));
            let month = month(argv.get(4));
            let mut gd = GroupDate::new(
                group_id.try_into()?,
                YearMonth::new(year, month).expect("bad year/month"),
                baglama.clone(),
            );
            let _ = gd.set_group_status("GENERATING PAGE LIST", 0, "").await;
            gd.create_sqlite().await?;
        }
        Some("next") => {
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
        Some("next_all_seq") => {
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
        Some("next_all") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            process_all_groups(year, month, baglama.clone(), false).await?;
        }
        Some("backfill") => {
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
        Some("test") => {
            let current_month = chrono::Utc::now().month();
            info!("{current_month}");
        }
        other => panic!("Action required (not {:?}", other),
    }
    Ok(())
}
