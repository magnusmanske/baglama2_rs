//! Database connections: the tool DB and the Commons replicas, with the
//! retry and timeout handling a long batch job on Toolforge needs.

use crate::config::Config;
use anyhow::{anyhow, Result};
use log::warn;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use wikimisc::mysql_async::{self, from_row_opt, prelude::*, Conn};
use wikimisc::toolforge::db::Cluster;
#[cfg(test)]
use wikimisc::toolforge::db::ExtensionDb;
use wikimisc::toolforge::pool::{ToolsDbPool, WikiPool};
use wikimisc::toolforge::Error;

/// The wiki whose replica databases this tool reads. Baglama only queries
/// Commons.
const COMMONS_WIKI: &str = "commonswiki";

/// The tool DB on ToolsDB, after the tool's database user:
/// `<user>__baglama2_p`.
const TOOLDB_SUFFIX: &str = "__baglama2_p";

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

/// The database connections. Hosts and credentials come from the `toolforge`
/// crate: on Toolforge from the tool's environment or `replica.my.cnf`;
/// locally from a `replica.my.cnf` with `local='true'`, whose `[tunnels]`
/// section gives the ports `connect_db.sh` opens.
#[derive(Debug)]
pub struct Db {
    /// The Wiki Replicas (web cluster), of which only Commons is read.
    replicas: WikiPool,
    tooldb: ToolsDbPool,
    /// The tool DB's name, see [`TOOLDB_SUFFIX`].
    tooldb_name: String,
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

    /// Connections per replica server (the Commons core section, and the
    /// links cluster): the replicas allow a tool 10 per server.
    const REPLICA_POOL_MAX: usize = 10;

    /// Connections to the tool DB.
    const TOOLDB_POOL_MAX: usize = 20;

    /// Server-side time limit for each attempt of a Commons query, in seconds;
    /// one entry per attempt. Grows because a query that just missed the limit
    /// on a loaded replica usually succeeds with a bit more time.
    const DB_QUERY_TIME_LIMITS: [u64; 5] = [600, 900, 1200, 1500, 1800];

    /// Extra time the client waits beyond the server-side limit before giving
    /// up itself. Only a backstop: normally the server ends the query first.
    const DB_QUERY_CLIENT_GRACE: Duration = Duration::from_secs(60);

    /// Sets up the pools, looking up the credentials as the `toolforge`
    /// crate does (`TOOL_*` variables, then `replica.my.cnf` in
    /// `$TOOL_DATA_DIR` or the home directory). Connects to nothing:
    /// connections are opened on first use.
    pub fn new(config: &Config) -> Result<Self> {
        Self::with_my_cnf(config, None)
    }

    /// [`Self::new`], but with the credentials from `my_cnf` if given.
    fn with_my_cnf(config: &Config, my_cnf: Option<&Path>) -> Result<Self> {
        let mut replicas = WikiPool::builder(Cluster::Web);
        let mut tooldb = ToolsDbPool::builder();
        if let Some(path) = my_cnf {
            replicas = replicas.my_cnf_path(path);
            tooldb = tooldb.my_cnf_path(path);
        }
        let replicas = replicas
            .pool_max(Self::REPLICA_POOL_MAX)
            .connect_timeout(Self::DB_CONN_TIMEOUT)
            .build()?;
        let tooldb = tooldb
            .pool_max(Self::TOOLDB_POOL_MAX)
            .connect_timeout(Self::DB_CONN_TIMEOUT)
            .build()?;
        let tooldb_name = format!("{}{TOOLDB_SUFFIX}", tooldb.user());
        Ok(Self {
            replicas,
            tooldb,
            tooldb_name,
            hold_on: config.hold_on(),
        })
    }

    /// Acquire a connection with `connect`, retrying transient failures with
    /// backoff and capping each attempt so it can't block forever (the prod-on-
    /// Toolforge failure mode). Toolforge replicas occasionally refuse or are
    /// slow to hand out a connection; a single 30s cap with no retry was too
    /// brittle for a long-running batch job. Total budget is bounded:
    /// `DB_CONN_RETRIES` attempts of up to `DB_CONN_TIMEOUT` each, plus
    /// backoff, so it still fails loudly rather than hanging indefinitely.
    ///
    /// Refusals for `max_user_connections` don't count as attempts: they mean
    /// the tool's connections are all busy (possibly in another job), so this
    /// waits for one to free up, for up to `DB_CONN_LIMIT_WAIT`. Errors that
    /// a retry can't fix, e.g. tables no single Commons database holds, are
    /// returned at once.
    async fn get_conn_with_retry<F, Fut>(&self, name: &str, connect: F) -> Result<Conn>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Conn, Error>>,
    {
        let started = std::time::Instant::now();
        let mut failures = 0;
        let mut limit_waits = 0;
        loop {
            let err = match connect().await {
                Ok(conn) => return Ok(conn),
                Err(Error::MysqlError(e)) if is_server_error(&e, ER_USER_LIMIT_REACHED) => {
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
                // `ConnectTimeout` comes after `DB_CONN_TIMEOUT`.
                Err(e @ (Error::MysqlError(_) | Error::ConnectTimeout(_))) => e.to_string(),
                Err(e) => return Err(anyhow!("'{name}' database: {e}")),
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
        let mut conn = self
            .get_conn_with_retry("tooldb", || self.tooldb.connect(&self.tooldb_name))
            .await?;
        conn.query_drop("SET NAMES utf8mb4").await?;
        Ok(conn)
    }

    /// A connection to the Commons replica database that can serve a query
    /// reading all of `tables`.
    ///
    /// Commons had its links tables moved to a separate cluster (`x4`) in
    /// September 2026, so "the Commons connection" is no longer a single
    /// thing. [`WikiPool::connect_for_tables`] picks the database holding the
    /// tables, so a query keeps working when tables move again; the core
    /// database's copies of split-off tables are frozen.
    ///
    /// Tables that no single database holds — a join across the split — are
    /// an error; such joins must be done in code.
    pub async fn get_commons_conn_for_tables(&self, tables: &[&str]) -> Result<Conn> {
        self.get_conn_with_retry("commons", || {
            self.replicas.connect_for_tables(COMMONS_WIKI, tables)
        })
        .await
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
    /// `WikiPool::connect_for_tables` routes queries by the tables they read.
    /// Pins down where the table sets this tool queries go.
    #[test]
    fn test_commons_tables_routing() {
        let route = |tables: &[&str]| ExtensionDb::for_tables(COMMONS_WIKI, tables);
        // `page` exists on both clusters; every other links table only on the
        // links cluster, so a join over them belongs there.
        for tables in [
            &["page", "categorylinks", "linktarget"][..],
            &["globalimagelinks"],
        ] {
            assert_eq!(
                route(tables).unwrap(),
                Some(ExtensionDb::Links),
                "{tables:?}"
            );
        }
        // Tables that stayed on the core cluster; `page` alone resolves to
        // core, the first that holds it.
        for tables in [&["image", "actor", "user"][..], &["sites"], &["page"]] {
            assert_eq!(route(tables).unwrap(), None, "{tables:?}");
        }
        // A query spanning the split cannot be served by one connection, so
        // it is an error rather than a silently wrong result.
        assert!(route(&["actor", "pagelinks"]).is_err());
    }

    /// The tool DB is named after the tool's database user, and setting up
    /// the pools reads the credentials but connects to nothing.
    #[test]
    fn test_with_my_cnf() {
        let dir = std::env::temp_dir().join(format!("baglama2-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let my_cnf = dir.join("replica.my.cnf");
        std::fs::write(&my_cnf, "[client]\nuser='s12345'\npassword='s3cr3t'\n").unwrap();
        let config: Config = serde_json::from_value(serde_json::json!({
            "gzb_data_root_path": "/x/gzb"
        }))
        .unwrap();
        let db = Db::with_my_cnf(&config, Some(&my_cnf)).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(db.tooldb_name, "s12345__baglama2_p");
        let debug = format!("{db:?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
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
