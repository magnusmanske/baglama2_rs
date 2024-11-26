use anyhow::Result;
use chrono::Datelike;
use chrono::{DateTime, Months, Utc};
use std::env;
use std::sync::{Arc, Mutex};

pub mod auxiliary;
pub mod baglama2;
pub mod group_id;
pub mod groupdate;

use auxiliary::*;
use baglama2::*;
pub use group_id::GroupId;
use groupdate::*;

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
            let last: DateTime<Utc> = Utc::now().checked_sub_months(Months::new(1)).unwrap();
            last.month()
        }
        Some(s) => s.parse::<u32>().expect("Month: number expected, not {s}"),
        None => panic!("Month expected but missing"),
    }
}

fn year(year: Option<&String>) -> i32 {
    match year.map(|s| s.as_str()) {
        Some("lm") => {
            let last: DateTime<Utc> = Utc::now().checked_sub_months(Months::new(1)).unwrap();
            last.year()
        }
        Some(s) => s.parse::<i32>().expect("Year: number expected, not {s}"),
        None => panic!("Year expected but missing"),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let argv: Vec<String> = env::args_os().map(|s| s.into_string().unwrap()).collect();
    let baglama = Arc::new(Baglama2::new().await?);
    match argv.get(1).map(|s| s.as_str()) {
        Some("run") => {
            let group_id = argv
                .get(2)
                .map(|s| s.parse::<usize>().unwrap())
                .expect("Group ID expected");
            let year = year(argv.get(3));
            let month = month(argv.get(4));
            let mut gd = GroupDate::new(group_id.into(), YearMonth::new(year, month));
            let _ = gd
                .set_group_status("GENERATING PAGE LIST", 0, "", &baglama)
                .await;
            gd.create_sqlite(&baglama).await?;
        }
        Some("next") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            if let Some(group_id) = baglama.get_next_group_id(year, month).await {
                GroupDate::new(group_id.into(), YearMonth::new(year, month))
                    .create_sqlite(&baglama)
                    .await?;
            } else {
                println!("No more groups for {year}/{month}");
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
                if let Some(group_id) = baglama.get_next_group_id(year, month).await {
                    let mut gd = GroupDate::new(group_id.into(), YearMonth::new(year, month));
                    let _ = gd
                        .set_group_status("GENERATING PAGE LIST", 0, "", &baglama)
                        .await;
                    match gd.create_sqlite(&baglama).await {
                        Ok(_) => {}
                        Err(err) => {
                            let _ = gd.set_group_status("FAILED", 0, "", &baglama).await;
                            println!("{group_id} failed: {:?}", &err);
                        }
                    }
                } else {
                    println!("No more groups for {year}/{month}");
                    break;
                }
            }
        }
        Some("next_all") => {
            let year = year(argv.get(2));
            let month = month(argv.get(3));
            if argv.get(4).is_none() {
                // Any third parameter will do this
                baglama.clear_incomplete_group_status(year, month).await?;
            }
            let max_concurrent = baglama.config()["max_concurrent_jobs"].as_u64().unwrap();
            let concurrent: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
            loop {
                if *concurrent.lock().unwrap() >= max_concurrent {
                    baglama.hold_on();
                    continue;
                }
                let group_id_opt = baglama.get_next_group_id(year, month).await;
                if let Some(group_id) = group_id_opt {
                    let concurrent = concurrent.clone();
                    let baglama = baglama.clone();
                    *concurrent.lock().unwrap() += 1;
                    println!("Now {} jobs running", concurrent.lock().unwrap());
                    let mut gd = GroupDate::new(group_id.into(), YearMonth::new(year, month));
                    let _ = gd
                        .set_group_status("GENERATING PAGE LIST", 0, "", &baglama)
                        .await;
                    tokio::spawn(async move {
                        match gd.create_sqlite(&baglama).await {
                            Ok(_) => {}
                            Err(err) => {
                                let _ = gd.set_group_status("FAILED", 0, "", &baglama).await;
                                println!("{group_id} failed: {:?}", &err);
                            }
                        }
                        drop(gd);
                        *concurrent.lock().unwrap() -= 1;
                    });
                } else {
                    // Wait for thrads to finish
                    if *concurrent.lock().unwrap() > 0 {
                        baglama.hold_on();
                        continue;
                    }
                    println!("Complete");
                    break;
                }
            }
        }
        other => panic!("Action required (not {:?}", other),
    }
    Ok(())
}

/*
ssh magnus@tools-login.wmflabs.org -L 3311:oxztsldqokc.svc.trove.eqiad1.wikimedia.cloud:3306 -N & \
ssh magnus@tools-login.wmflabs.org -L 3310:commonswiki.web.db.svc.eqiad.wmflabs:3306 -N &

*/
