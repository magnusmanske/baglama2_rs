use anyhow::{anyhow, Result};
use baglama2::*;
use chrono::{DateTime, Datelike, Months, Utc};
use log::{error, info};
use site::Site;
use std::env;
use std::future::Future;
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use year_month::YearMonth;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub type DbId = usize;

mod baglama2;
mod global_image_links;
mod gzb;
mod pageviews;
mod row_group;
mod site;
mod year_month;

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

/// Run an async step with a wall-clock timeout. On timeout, logs and
/// returns an error so the process exits with a clear message instead of
/// hanging silently (the prod-on-Toolforge failure mode). Wrapping a step
/// also bounds any network/DB call inside it that has no timeout of its own.
async fn with_timeout<F, T>(label: &str, secs: u64, fut: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    info!("[{label}] starting (timeout {secs}s)");
    match tokio::time::timeout(Duration::from_secs(secs), fut).await {
        Ok(res) => {
            info!("[{label}] finished");
            res
        }
        Err(_) => {
            error!("[{label}] TIMED OUT after {secs}s — aborting");
            Err(anyhow!("Step '{label}' timed out after {secs}s"))
        }
    }
}

/// Value of `--name=VALUE` or `--name VALUE`, anywhere on the command line.
fn flag_value(argv: &[String], name: &str) -> Option<String> {
    let prefix = format!("--{name}=");
    let bare = format!("--{name}");
    let mut iter = argv.iter();
    while let Some(arg) = iter.next() {
        if let Some(v) = arg.strip_prefix(&prefix) {
            return Some(v.to_string());
        }
        if *arg == bare {
            return iter.next().cloned();
        }
    }
    None
}

fn has_flag(argv: &[String], name: &str) -> bool {
    let bare = format!("--{name}");
    argv.contains(&bare)
}

fn parsed_flag<T: std::str::FromStr>(argv: &[String], name: &str) -> Option<T> {
    flag_value(argv, name).map(|v| {
        v.parse()
            .unwrap_or_else(|_| panic!("--{name}: cannot parse '{v}'"))
    })
}

/// `--groups=1,2,3`
fn group_ids_flag(argv: &[String]) -> Option<Vec<usize>> {
    flag_value(argv, "groups").map(|v| {
        v.split(',')
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse()
                    .unwrap_or_else(|_| panic!("--groups: bad id '{s}'"))
            })
            .collect()
    })
}

/// Arguments that are not `--flags` (nor a flag's separate value).
fn positional(argv: &[String]) -> Vec<String> {
    const VALUE_FLAGS: &[&str] = &[
        "--dump",
        "--groups",
        "--list-jobs",
        "--build-jobs",
        "--max",
        "--out",
    ];
    let mut ret = vec![];
    let mut iter = argv.iter();
    while let Some(arg) = iter.next() {
        if VALUE_FLAGS.contains(&arg.as_str()) {
            iter.next();
        } else if !arg.starts_with("--") {
            ret.push(arg.clone());
        }
    }
    ret
}

const USAGE: &str = "\
gzb commands (view data as one compressed file per group-month):
  gzb_check YEAR MONTH [--dump=PATH]
      Check dump, replicas, tool DB and output dirs for a month; changes nothing.
  gzb_month YEAR MONTH [--dump=PATH] [--groups=1,2] [--force] [--list-jobs=6]
                       [--build-jobs=3] [--keep-work] [--no-check]
      Generate a month for all active groups (or --groups). Runs gzb_check
      first and stops on any problem; --no-check skips that. Resumable: re-run
      the same command after a failure. --force replaces complete data.
  gzb_show GROUP YEAR MONTH [WIKI] [--max=20]
      Print a gzb file's per-wiki totals, or one wiki's top pages.
  gzb_tsv GROUP YEAR MONTH [WIKI] [--out=FILE]
      Export a gzb file (or one wiki of it) as tab-separated text, with
      '#' metadata lines above the header. Writes to stdout without --out.
YEAR and MONTH may be 'lm' for last month.";

/// Extract an optional dump-file override from the command line.
/// Accepts both `--dump=PATH` and `--dump PATH` (space-separated). The flag
/// may appear at any position; positional args are unaffected.
fn dump_override(argv: &[String]) -> Option<PathBuf> {
    let mut iter = argv.iter();
    while let Some(arg) = iter.next() {
        if let Some(path) = arg.strip_prefix("--dump=") {
            return Some(PathBuf::from(path));
        }
        if arg == "--dump" {
            return iter.next().map(PathBuf::from);
        }
    }
    None
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Unconditional startup banner to STDOUT (line-buffered, flushes on
    // newline). If this line does not appear in the captured output, the
    // running binary is not this build, or output is not being captured —
    // before debugging logic, fix that. Includes the build version so a
    // stale deploy is obvious. To stderr when stdout carries data (gzb_tsv
    // without --out).
    let args: Vec<String> = env::args().skip(1).collect();
    let banner = format!(
        "baglama2 v{} starting — args: {args:?}",
        env!("CARGO_PKG_VERSION")
    );
    if args.first().is_some_and(|c| c == "gzb_tsv") && !args.iter().any(|a| a.starts_with("--out"))
    {
        eprintln!("{banner}");
    } else {
        println!("{banner}");
    }

    // Install a logger backend. Without this, all log::{info,warn,error,trace}
    // macros are silently discarded. Defaults to `info`; override per-module
    // via the RUST_LOG env var (e.g. `RUST_LOG=baglama2=trace`).
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let argv: Vec<String> = env::args_os()
        .map(|s| s.into_string().expect("Bad argv"))
        .collect();
    let command = argv.get(1).map(|s| s.as_str()).unwrap_or_default();
    if command.is_empty() || command == "help" || command == "--help" {
        println!("{USAGE}");
        return Ok(());
    }
    if !command.starts_with("gzb_") {
        return Err(anyhow!("Unknown command '{command}'\n{USAGE}"));
    }
    info!("Starting up; initializing Baglama2 (config + DB pool + Wikidata API)");
    let baglama = Arc::new(with_timeout("Baglama2::new", 600, Baglama2::new()).await?);
    run_gzb_command(command, &argv, baglama).await
}

async fn run_gzb_command(command: &str, argv: &[String], baglama: Arc<Baglama2>) -> Result<()> {
    let pos = positional(argv);
    let ym_at = |i: usize| {
        YearMonth::new(year(pos.get(i)), month(pos.get(i + 1)))
            .unwrap_or_else(|e| panic!("bad year/month: {e}"))
    };
    match command {
        "gzb_check" | "gzb_month" => {
            let ym = ym_at(2);
            let opts = gzb::month::MonthOptions {
                dump_override: dump_override(argv),
                group_ids: group_ids_flag(argv),
                force: has_flag(argv, "force"),
                list_jobs: parsed_flag(argv, "list-jobs").unwrap_or(6),
                build_jobs: parsed_flag(argv, "build-jobs").unwrap_or(3),
                keep_work: has_flag(argv, "keep-work"),
                no_check: has_flag(argv, "no-check"),
            };
            let job = gzb::month::GzbMonth::new(baglama.clone(), ym, opts);
            if command == "gzb_check" {
                let report = job.check().await?;
                if !report.problems.is_empty() {
                    return Err(anyhow!("{} problem(s)", report.problems.len()));
                }
                return Ok(());
            }
            // As before every monthly run.
            with_timeout(
                "deactivate_nonexistent_categories",
                600,
                baglama.deactivate_nonexistent_categories(),
            )
            .await?;
            job.run().await
        }
        "gzb_tsv" => {
            let group_id: usize = pos
                .get(2)
                .and_then(|s| s.parse().ok())
                .expect("group ID expected");
            let ym = ym_at(3);
            let path = gzb::gzb_path(&baglama.gzb_data_root_path(), group_id, &ym);
            let mut reader = gzb::GzbReader::open(&path)?;
            let group_label = baglama
                .get_group(&GroupId::try_from(group_id)?)
                .await
                .ok()
                .flatten()
                .map(|g| {
                    if g.is_user_name() {
                        format!("files uploaded by User:{}", g.category())
                    } else {
                        format!("Category:{} (depth {})", g.category(), g.depth())
                    }
                });
            let meta = gzb::tsv::TsvMeta {
                group_label,
                wiki: pos.get(5).cloned(),
            };
            let rows = match flag_value(argv, "out") {
                Some(out) => {
                    let file = std::fs::File::create(&out)?;
                    let rows =
                        gzb::tsv::export(&mut reader, &meta, &mut std::io::BufWriter::new(file))?;
                    println!("{rows} rows written to {out}");
                    rows
                }
                None => {
                    let stdout = std::io::stdout();
                    gzb::tsv::export(&mut reader, &meta, &mut stdout.lock())?
                }
            };
            info!("gzb_tsv: {rows} rows");
            Ok(())
        }
        "gzb_show" => {
            let group_id: usize = pos
                .get(2)
                .and_then(|s| s.parse().ok())
                .expect("group ID expected");
            let ym = ym_at(3);
            let path = gzb::gzb_path(&baglama.gzb_data_root_path(), group_id, &ym);
            let mut reader = gzb::GzbReader::open(&path)?;
            let h = reader.header().clone();
            match pos.get(5) {
                None => {
                    println!(
                        "{}: group {} {}-{:02}, source {}, created {}\n{} pages, {} views, {} wikis",
                        path.display(),
                        h.group_id,
                        h.year,
                        h.month,
                        h.source,
                        h.created,
                        h.total_pages,
                        h.total_views,
                        h.sites.len()
                    );
                    for site in &h.sites {
                        println!("{}\t{}\t{}", site.giu, site.pages, site.views);
                    }
                }
                Some(giu) => {
                    let max = parsed_flag(argv, "max").unwrap_or(20);
                    for row in reader.rows(giu, max)? {
                        println!(
                            "{}\t{}\t{}\t{}",
                            row.views,
                            row.namespace_id,
                            row.title,
                            row.files.join("|")
                        );
                    }
                }
            }
            Ok(())
        }
        other => Err(anyhow!("Unknown command '{other}'\n{USAGE}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flags_and_positional() {
        let a = argv(&[
            "bin",
            "gzb_month",
            "2026",
            "9",
            "--groups=1,2",
            "--force",
            "--dump",
            "/x.bz2",
            "--build-jobs=2",
            "--no-check",
        ]);
        assert_eq!(positional(&a), vec!["bin", "gzb_month", "2026", "9"]);
        assert_eq!(group_ids_flag(&a), Some(vec![1, 2]));
        assert!(has_flag(&a, "force"));
        assert!(!has_flag(&a, "keep-work"));
        assert!(has_flag(&a, "no-check"));
        assert_eq!(dump_override(&a), Some(PathBuf::from("/x.bz2")));
        assert_eq!(parsed_flag::<usize>(&a, "build-jobs"), Some(2));
        assert_eq!(parsed_flag::<usize>(&a, "list-jobs"), None);
    }

    #[test]
    fn test_month_numeric() {
        assert_eq!(month(Some(&"3".to_string())), 3);
        assert_eq!(month(Some(&"12".to_string())), 12);
        assert_eq!(month(Some(&"1".to_string())), 1);
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_dump_override_equals_form() {
        let a = argv(&["bin", "gzb_month", "2026", "5", "--dump=/tmp/d.bz2"]);
        assert_eq!(dump_override(&a), Some(PathBuf::from("/tmp/d.bz2")));
    }

    #[test]
    fn test_dump_override_space_form() {
        let a = argv(&["bin", "gzb_month", "2026", "5", "--dump", "/tmp/d.bz2"]);
        assert_eq!(dump_override(&a), Some(PathBuf::from("/tmp/d.bz2")));
    }

    #[test]
    fn test_dump_override_absent() {
        let a = argv(&["bin", "gzb_month", "2026", "5"]);
        assert_eq!(dump_override(&a), None);
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
