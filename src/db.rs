//! Database connections: the tool DB and the Commons replicas, with the
//! retry and timeout handling a long batch job on Toolforge needs.

use crate::config::Config;
use anyhow::{anyhow, Result};
use log::warn;
use mysql_async::{from_row_opt, prelude::*, Conn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use wikimisc::toolforge_db::{DatabaseError, DbCluster, ToolforgeDB};

/// The wiki whose replica databases this tool reads. Baglama only queries
/// Commons; the links split below applies to no other wiki.
const COMMONS_WIKI: &str = "commonswiki";

/// Pool key of the Commons core replica (`commonswiki…`), holding every table
/// that was not split off.
const POOL_COMMONS_CORE: &str = "commons";

/// Pool key of the Commons links extension cluster (`links.commonswiki…`, the
/// `x4` section), which took over the links tables in September 2026.
///
/// A future split needs a new pool under its own key, registered in
/// [`Db::new`] from config, plus a [`DbCluster`] arm in
/// [`Db::commons_pool_key`].
const POOL_COMMONS_LINKS: &str = "commons_links";

/// MySQL/MariaDB error: the user has used up `max_user_connections`.
const ER_USER_LIMIT_REACHED: u16 = 1226;

/// MariaDB error: the query ran past `max_statement_time`.
const ER_STATEMENT_TIMEOUT: u16 = 1969;

/// Whether `e` is an error the server reported with `code`.
fn is_server_error(e: &mysql_async::Error, code: u16) -> bool {
    matches!(e, mysql_async::Error::Server(se) if se.code == code)
}

pub fn sql_placeholders(num: usize) -> String {
    let mut placeholders = "?,".repeat(num);
    placeholders.pop();
    placeholders
}

pub fn value2opt_string(value: &mysql_async::Value) -> Option<String> {
    match value {
        mysql_async::Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

#[derive(Debug)]
pub struct Db {
    tfdb: ToolforgeDB,
    hold_on: Duration,
}

impl Db {
    /// Max time for a SINGLE connection-acquisition attempt. Generous because
    /// a cold pool / slow replica handshake on Toolforge can legitimately take
    /// tens of seconds; normal is well under a second.
    const DB_CONN_TIMEOUT: Duration = Duration::from_secs(60);

    /// How many times to retry connection acquisition before giving up.
    const DB_CONN_RETRIES: u32 = 4;

    /// How long to keep waiting for a connection while the server refuses it
    /// with `max_user_connections` (error 1226). The limit is per tool user
    /// across all processes, so it frees up as other queries end; waiting is
    /// better than failing a group.
    const DB_CONN_LIMIT_WAIT: Duration = Duration::from_secs(60 * 60);

    /// Server-side time limit for each attempt of a Commons query, in seconds;
    /// one entry per attempt. Grows because a query that just missed the limit
    /// on a loaded replica usually succeeds with a bit more time.
    const DB_QUERY_TIME_LIMITS: [u64; 5] = [600, 900, 1200, 1500, 1800];

    /// Extra time the client waits beyond the server-side limit before giving
    /// up itself. Only a backstop: normally the server ends the query first.
    const DB_QUERY_CLIENT_GRACE: Duration = Duration::from_secs(60);

    /// Registers the pools from `config`. Connects to nothing: connections
    /// are opened on first use.
    pub fn new(config: &Config) -> Result<Self> {
        let mut tfdb = ToolforgeDB::default();
        tfdb.add_mysql_pool("tooldb", &config.tooldb)?;
        tfdb.add_mysql_pool(POOL_COMMONS_CORE, &config.commons)?;
        // A missing `commons_links` entry must fail startup, not the first
        // links query, so say what to add.
        tfdb
            .add_mysql_pool(POOL_COMMONS_LINKS, &config.commons_links)
            .map_err(|e| {
                anyhow!(
                    "config needs a '{POOL_COMMONS_LINKS}' pool for the Commons links \
                     cluster (links.commonswiki…, the x4 split), e.g. \
                     `\"{POOL_COMMONS_LINKS}\": {{ \"url\": \"mysql://USER:PASS@links.commonswiki.web.db.svc.wikimedia.cloud:3306/commonswiki_p\" }}`: {e}"
                )
            })?;
        Ok(Self {
            tfdb,
            hold_on: config.hold_on(),
        })
    }

    /// Acquire a pooled connection, retrying transient failures with backoff
    /// and capping each attempt so it can't block forever (the prod-on-
    /// Toolforge failure mode). Toolforge replicas occasionally refuse or are
    /// slow to hand out a connection; a single 30s cap with no retry was too
    /// brittle for a long-running batch job. Total budget is bounded:
    /// `DB_CONN_RETRIES` attempts of up to `DB_CONN_TIMEOUT` each, plus
    /// backoff, so it still fails loudly rather than hanging indefinitely.
    ///
    /// Refusals for `max_user_connections` don't count as attempts: they mean
    /// the tool's connections are all busy (possibly in another job), so this
    /// waits for one to free up, for up to `DB_CONN_LIMIT_WAIT`.
    async fn get_conn_with_timeout(&self, name: &'static str) -> Result<Conn> {
        let started = std::time::Instant::now();
        let mut failures = 0;
        let mut limit_waits = 0;
        loop {
            let err =
                match tokio::time::timeout(Self::DB_CONN_TIMEOUT, self.tfdb.get_connection(name))
                    .await
                {
                    Ok(Ok(conn)) => return Ok(conn),
                    Ok(Err(DatabaseError::MySql(e)))
                        if is_server_error(&e, ER_USER_LIMIT_REACHED) =>
                    {
                        if started.elapsed() >= Self::DB_CONN_LIMIT_WAIT {
                            return Err(anyhow!(
                                "Failed to acquire '{name}' database connection: still at the \
                             connection limit after {}s ({e})",
                                started.elapsed().as_secs()
                            ));
                        }
                        limit_waits += 1;
                        // 30s, doubling up to 5 min.
                        let wait = Duration::from_secs((15u64 << limit_waits.min(5)).min(300));
                        warn!(
                            "get_conn '{name}': at the connection limit; waiting {}s",
                            wait.as_secs()
                        );
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    Ok(Err(e)) => e.to_string(),
                    Err(_) => format!("timed out after {}s", Self::DB_CONN_TIMEOUT.as_secs()),
                };
            failures += 1;
            warn!(
                "get_conn '{name}': attempt {failures}/{} failed: {err}",
                Self::DB_CONN_RETRIES
            );
            if failures >= Self::DB_CONN_RETRIES {
                return Err(anyhow!(
                    "Failed to acquire '{name}' database connection after {} attempts ({err})",
                    Self::DB_CONN_RETRIES
                ));
            }
            // Linear-ish backoff: 3s, 6s, 9s, …
            tokio::time::sleep(Duration::from_secs(3 * failures as u64)).await;
        }
    }

    /// A tool DB connection that reads and writes UTF-8.
    ///
    /// A new connection asks for utf8mb4, but a pooled one comes back reset
    /// to the server's default character set. On a server whose default is
    /// `latin1` (the tool DB's previous host), non-ASCII text would then be
    /// read as latin1 and written double-encoded. ToolsDB defaults to
    /// utf8mb4, but this does not depend on that. (Older queries sidestep it
    /// with `FROM_BASE64(TO_BASE64(...))`.)
    pub async fn get_tooldb_conn(&self) -> Result<Conn> {
        let mut conn = self.get_conn_with_timeout("tooldb").await?;
        conn.query_drop("SET NAMES utf8mb4").await?;
        Ok(conn)
    }

    /// A connection to the Commons replica cluster that can serve a query
    /// reading all of `tables`.
    ///
    /// Commons had its links tables moved to a separate cluster (`x4`) in
    /// September 2026, so "the Commons connection" is no longer a single
    /// thing. The cluster is chosen with [`DbCluster::for_tables`] instead of
    /// being hard-coded, so a query keeps working when tables move again.
    ///
    /// Tables that no single cluster holds — a join across the split — are an
    /// error; such joins must be done in code.
    pub async fn get_commons_conn_for_tables(&self, tables: &[&str]) -> Result<Conn> {
        self.get_conn_with_timeout(Self::commons_pool_key_for_tables(tables)?)
            .await
    }

    /// The pool key serving a query over `tables` on Commons, resolved through
    /// [`DbCluster`].
    ///
    /// Kept pure so the table-to-cluster mapping can be unit-tested without a
    /// database; the `&'static str` is what `get_conn_with_timeout` needs.
    fn commons_pool_key_for_tables(tables: &[&str]) -> Result<&'static str> {
        Ok(Self::commons_pool_key(DbCluster::for_tables(
            COMMONS_WIKI,
            tables,
        )?))
    }

    /// Maps a Commons cluster to the pool key registered for it. `Core` and
    /// `Links` are the two clusters Commons has; a future split adds a pool
    /// key and an arm here.
    fn commons_pool_key(cluster: DbCluster) -> &'static str {
        match cluster {
            DbCluster::Core => POOL_COMMONS_CORE,
            DbCluster::Links => POOL_COMMONS_LINKS,
            // The term store is Wikidata-only, so `for_tables` never returns
            // it for Commons; fall back to core rather than inventing a pool.
            DbCluster::TermStore => POOL_COMMONS_CORE,
        }
    }

    /// Runs a read query on the Commons cluster that holds `tables`, retrying
    /// failures, and returns all rows. See [`Self::fold_commons`].
    pub async fn query_commons<T, P>(&self, tables: &[&str], sql: &str, params: P) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Clone + Send,
    {
        self.fold_commons(tables, sql, params, Vec::new, |rows, row| {
            rows.push(row);
            Ok(())
        })
        .await
    }

    /// Runs a read query and calls `f` for each row as it arrives, so the
    /// result is never held in memory. A failed attempt is retried from the
    /// start, so `f` can see a row again; callers must tolerate that. An
    /// error from `f` ends the query and is not retried.
    pub async fn query_commons_each<T, P, F>(
        &self,
        tables: &[&str],
        sql: &str,
        params: P,
        mut f: F,
    ) -> Result<()>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Clone + Send,
        F: FnMut(T) -> Result<()>,
    {
        self.fold_commons(tables, sql, params, || (), |(), row| f(row))
            .await
    }

    /// The Commons read queries: every attempt starts from `init()` and
    /// folds each row into it with `step`, as the rows stream in.
    ///
    /// Each attempt sets MariaDB's `max_statement_time`, so the server ends a
    /// query that runs too long and its connection is free again. A query the
    /// client merely gives up on keeps running on the server and keeps holding
    /// one of the tool's few connections, so retries would pile up until the
    /// server refuses new ones (`max_user_connections`). The client-side
    /// timeout is only a backstop, slightly longer than the server's limit.
    async fn fold_commons<T, P, A, I, S>(
        &self,
        tables: &[&str],
        sql: &str,
        params: P,
        init: I,
        mut step: S,
    ) -> Result<A>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Clone + Send,
        I: Fn() -> A,
        S: FnMut(&mut A, T) -> Result<()>,
    {
        let attempts = Self::DB_QUERY_TIME_LIMITS.len();
        let mut last_err = anyhow!("no attempt made");
        for (attempt, &limit) in Self::DB_QUERY_TIME_LIMITS.iter().enumerate() {
            if attempt > 0 {
                warn!(
                    "query_commons: attempt {attempt}/{attempts} failed ({last_err:#}); retrying"
                );
                self.hold_on().await;
            }
            let mut conn = match self.get_commons_conn_for_tables(tables).await {
                Ok(conn) => conn,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            if let Err(e) = conn
                .query_drop(format!("SET SESSION max_statement_time={limit}"))
                .await
            {
                // Not MariaDB, or the connection is broken; the client-side
                // timeout still applies.
                warn!("query_commons: cannot set max_statement_time: {e}");
            }
            let query = async {
                let mut result = conn.exec_iter(sql, params.clone()).await?;
                let mut acc = init();
                let mut failed: Option<anyhow::Error> = None;
                while let Some(row) = result.next().await? {
                    let outcome = match from_row_opt::<T>(row) {
                        Ok(row) => step(&mut acc, row),
                        // A row that does not fit `T` will not fit on a retry
                        // either. An error, not a panic: only this group fails.
                        Err(e) => Err(anyhow!("Commons query returned an unexpected row: {e}")),
                    };
                    if let Err(e) = outcome {
                        failed = Some(e);
                        break;
                    }
                }
                Ok::<_, mysql_async::Error>(match failed {
                    Some(e) => Err(e),
                    None => Ok(acc),
                })
            };
            let client_limit = Duration::from_secs(limit) + Self::DB_QUERY_CLIENT_GRACE;
            match tokio::time::timeout(client_limit, query).await {
                Ok(Ok(Ok(acc))) => return Ok(acc),
                Ok(Ok(Err(e))) => {
                    // Rows may be pending; don't hand the connection back.
                    tokio::spawn(conn.disconnect());
                    return Err(e);
                }
                Ok(Err(e)) if is_server_error(&e, ER_STATEMENT_TIMEOUT) => {
                    last_err = anyhow!("Commons query exceeded the server's {limit}s limit");
                }
                Ok(Err(e)) => last_err = e.into(),
                Err(_) => {
                    // The connection is mid-query; don't hand it back to the pool.
                    tokio::spawn(conn.disconnect());
                    last_err = anyhow!(
                        "Commons query timed out client-side after {}s",
                        client_limit.as_secs()
                    );
                }
            }
        }
        Err(last_err.context(format!("Commons query failed after {attempts} attempts")))
    }

    /// Server-side time limit for a streamed query ([`Self::stream_commons`]).
    /// The statement stays open while the caller works through the rows,
    /// which for the largest groups takes hours; the connection is in use
    /// the whole time, not abandoned, so the limit is only a backstop.
    const DB_STREAM_TIME_LIMIT_SECS: u64 = 6 * 60 * 60;

    /// Runs a read query in a background task and hands the rows over a
    /// channel holding at most `buffer` of them, so the caller can run its
    /// own queries between rows while the result is never held in memory
    /// (a single Commons category can have millions of files).
    ///
    /// No retry: a failure arrives as an `Err` and ends the stream; the
    /// caller's unit of work (a group) is what gets retried.
    pub fn stream_commons<T, P>(
        self: &Arc<Self>,
        tables: &[&str],
        sql: &str,
        params: P,
        buffer: usize,
    ) -> Result<mpsc::Receiver<Result<T>>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send + 'static,
    {
        let pool_key = Self::commons_pool_key_for_tables(tables)?;
        let (tx, rx) = mpsc::channel(buffer);
        let db = Arc::clone(self);
        let sql = sql.to_string();
        tokio::spawn(async move {
            let outcome: Result<()> = async {
                let mut conn = db.get_conn_with_timeout(pool_key).await?;
                conn.query_drop(format!(
                    "SET SESSION max_statement_time={}",
                    Self::DB_STREAM_TIME_LIMIT_SECS
                ))
                .await?;
                let mut result = conn.exec_iter(sql, params).await?;
                while let Some(row) = result.next().await? {
                    let row = from_row_opt::<T>(row)
                        .map_err(|e| anyhow!("Commons query returned an unexpected row: {e}"))?;
                    if tx.send(Ok(row)).await.is_err() {
                        // The receiver is gone; abandon the statement rather
                        // than read the rest of it.
                        drop(result);
                        tokio::spawn(conn.disconnect());
                        return Ok(());
                    }
                }
                Ok(())
            }
            .await;
            if let Err(e) = outcome {
                let _ = tx.send(Err(e)).await;
            }
        });
        Ok(rx)
    }

    /// Pause between retries, `hold_on` seconds from the config.
    pub async fn hold_on(&self) {
        tokio::time::sleep(self.hold_on).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value2opt_string_valid_utf8() {
        let value = mysql_async::Value::Bytes(b"k\xc3\xa9pe".to_vec()); // "képe" in UTF-8
        assert_eq!(value2opt_string(&value), Some("képe".to_string()));
    }

    #[test]
    fn test_value2opt_string_ascii() {
        let value = mysql_async::Value::Bytes(b"hello".to_vec());
        assert_eq!(value2opt_string(&value), Some("hello".to_string()));
    }

    #[test]
    fn test_value2opt_string_invalid_utf8_lossy() {
        // 0xff is not valid in any UTF-8 sequence; from_utf8_lossy replaces it with U+FFFD.
        let value = mysql_async::Value::Bytes(b"a\xffb".to_vec());
        assert_eq!(value2opt_string(&value), Some("a\u{FFFD}b".to_string()));
    }

    #[test]
    fn test_value2opt_string_non_bytes_returns_none() {
        let value = mysql_async::Value::NULL;
        assert_eq!(value2opt_string(&value), None);

        let value = mysql_async::Value::Int(42);
        assert_eq!(value2opt_string(&value), None);
    }

    #[test]
    fn test_value2opt_string_empty_bytes() {
        let value = mysql_async::Value::Bytes(vec![]);
        assert_eq!(value2opt_string(&value), Some(String::new()));
    }

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(sql_placeholders(50).len(), 99);
    }

    /// The Commons links tables moved to their own cluster in September 2026;
    /// queries must be routed by the tables they read, not by a single
    /// hard-coded Commons pool.
    #[test]
    fn test_commons_pool_key_for_tables() {
        // `page` exists on both clusters; every other links table only on the
        // links cluster, so a join over them belongs there.
        assert_eq!(
            Db::commons_pool_key_for_tables(&["page", "categorylinks", "linktarget"]).unwrap(),
            POOL_COMMONS_LINKS
        );
        assert_eq!(
            Db::commons_pool_key_for_tables(&["globalimagelinks"]).unwrap(),
            POOL_COMMONS_LINKS
        );

        // Tables that stayed on the core cluster keep using the core pool.
        assert_eq!(
            Db::commons_pool_key_for_tables(&["image", "actor", "user"]).unwrap(),
            POOL_COMMONS_CORE
        );
        assert_eq!(
            Db::commons_pool_key_for_tables(&["sites"]).unwrap(),
            POOL_COMMONS_CORE
        );
        // `page` alone still resolves to core, the first cluster that holds it.
        assert_eq!(
            Db::commons_pool_key_for_tables(&["page"]).unwrap(),
            POOL_COMMONS_CORE
        );

        // A query spanning the split cannot be served by one connection, so
        // it is an error rather than a silently wrong result.
        assert!(Db::commons_pool_key_for_tables(&["actor", "pagelinks"]).is_err());
    }

    // Pooled connections come back in the server default charset; see
    // `get_tooldb_conn`.
    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_tooldb_conn_utf8_after_reuse() {
        let db = Db::new(&Config::load().unwrap()).unwrap();
        for _ in 0..3 {
            let mut conn = db.get_tooldb_conn().await.unwrap();
            let name: Option<String> = conn
                .query_first("SELECT name FROM sites WHERE giu_code='vowiki'")
                .await
                .unwrap();
            assert_eq!(name.as_deref(), Some("Volapük"));
        }
    }

    // `query_commons` relies on the replica ending queries itself.
    #[tokio::test]
    #[ignore = "needs the DB tunnels from connect_db.sh"]
    async fn test_commons_max_statement_time() {
        let db = Db::new(&Config::load().unwrap()).unwrap();
        let mut conn = db.get_commons_conn_for_tables(&["page"]).await.unwrap();
        conn.query_drop("SET SESSION max_statement_time=1")
            .await
            .unwrap();
        let err = conn
            .query_drop("SELECT COUNT(*) FROM page a, page b")
            .await
            .unwrap_err();
        assert!(is_server_error(&err, ER_STATEMENT_TIMEOUT), "{err}");
    }
}
