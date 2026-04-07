//! Per-page REST API fallback for loading pageview counts.
//!
//! This is the slower path that fetches view counts one page at a time
//! via the Wikimedia per-article pageview REST API.  It works for months
//! whose dump has not been published yet (typically the current month).

use anyhow::Result;
use log::{error, info};
use mysql_async::prelude::Queryable;
use std::collections::HashMap;
use std::time::Duration;
use tools_interface::{Pageviews, PageviewsAccess, PageviewsAgent, PageviewsGranularity};

use crate::Baglama2;
use crate::YearMonth;

/// Batch size for each round of API fetches.
const VIEWDATA_BATCH_SIZE: usize = 1000;

/// Fetch missing pageview counts via the per-article REST API.
///
/// This loops in batches of `VIEWDATA_BATCH_SIZE`, fetching view counts
/// from the Wikimedia API and writing them back to the DB.
///
/// `table_name` is the viewdata table (e.g. `viewdata_2025_01`).
/// `flush_fn` is called with a batch of `(pages_id → views)` to write to DB.
pub async fn load_views_from_api<F, Fut>(
    baglama: &Baglama2,
    ym: &YearMonth,
    table_name: &str,
    flush_fn: F,
) -> Result<()>
where
    F: Fn(HashMap<usize, u64>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let pv = Pageviews::new(
        PageviewsGranularity::Monthly,
        PageviewsAccess::All,
        PageviewsAgent::User,
    );
    let sql = format!(
        "SELECT DISTINCT `vd`.`pages_id`,`server`,`title`
         FROM `{table_name}` AS `vd`,`pages`,`sites`
         WHERE `page_views` IS NULL
         AND `pages_id`=`pages`.`id`
         AND `pages`.`site`=`sites`.`id`
         LIMIT {VIEWDATA_BATCH_SIZE}"
    );
    let mut conn = baglama.get_tooldb_conn().await?;
    loop {
        let rows = conn
            .exec_iter(&sql, ())
            .await?
            .map_and_drop(mysql_async::from_row_opt::<(usize, String, String)>)
            .await?
            .into_iter()
            .filter_map(|row| row.ok())
            .collect::<Vec<_>>();
        if rows.is_empty() {
            break;
        }
        info!(
            "load_views_from_api: {} rows, starting with page_id {}",
            rows.len(),
            rows[0].0
        );

        let page2id = rows
            .into_iter()
            .filter(|(_id, _server, title)| !title.is_empty())
            .filter_map(|(id, server, title)| {
                let title_with_underscores = title.replace(' ', "_");
                let project = server.strip_suffix(".org")?.to_string();
                Some(((project, title_with_underscores), id))
            })
            .collect::<HashMap<(String, String), usize>>();
        let project_pages = page2id.keys().cloned().collect::<Vec<_>>();

        let results = pv
            .get_multiple_articles(
                &project_pages,
                &Pageviews::month_start(ym.year(), ym.month()).unwrap(),
                &Pageviews::month_end(ym.year(), ym.month()).unwrap(),
                5,
            )
            .await;
        let results = match results {
            Ok(results) => results,
            Err(e) => {
                error!("Error getting pageviews: {}. Waiting 5 seconds.", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let mut id2views: HashMap<usize, u64> = page2id.values().map(|id| (*id, 0)).collect();
        for result in results {
            let key = (result.project.to_owned(), result.article.to_owned());
            match page2id.get(&key) {
                Some(id) => {
                    id2views.insert(*id, result.total_views());
                }
                None => {
                    error!("Page not found: {:?}", key);
                }
            }
        }

        flush_fn(id2views).await?;
    }
    Ok(())
}
