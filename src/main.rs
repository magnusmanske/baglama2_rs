use anyhow::{anyhow, Result};
use baglama2::*;
use chrono::{DateTime, Datelike, Months, Utc};
use clap::{Args, Parser, Subcommand};
use config::Config;
use group_id::GroupId;
use log::{error, info, warn};
use row_group::RowGroup;
use site::Site;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use wiki::Dbname;
use year_month::YearMonth;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub type DbId = usize;

mod baglama2;
mod category;
mod config;
mod db;
mod global_image_links;
mod group_id;
mod group_status;
mod gzb;
mod pageviews;
mod row_group;
mod site;
mod wiki;
mod year_month;

/// How long `gzb_tsv` waits for the tool DB for its group label.
const LABEL_TIMEOUT: Duration = Duration::from_secs(10);

/// BaGLAMa 2 view data: one compressed gzb file per group and month.
#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check dump, replicas, tool DB and output dirs for a month; changes nothing.
    #[command(name = "gzb_check")]
    GzbCheck {
        #[command(flatten)]
        month: MonthArg,
        /// Pageview dump to use instead of the one on Toolforge's dumps mount.
        #[arg(long)]
        dump: Option<PathBuf>,
    },
    /// Generate a month for all active groups (or --groups).
    ///
    /// Runs gzb_check first and stops on any problem. Resumable: re-run the
    /// same command after a failure.
    #[command(name = "gzb_month")]
    GzbMonth {
        #[command(flatten)]
        month: MonthArg,
        #[command(flatten)]
        flags: MonthFlags,
    },
    /// Print a gzb file's per-wiki totals, or one wiki's top pages.
    #[command(name = "gzb_show")]
    GzbShow {
        group: GroupId,
        #[command(flatten)]
        month: MonthArg,
        /// Show this wiki's pages, e.g. enwiki.
        wiki: Option<Dbname>,
        /// Pages to show for WIKI.
        #[arg(long, default_value_t = 20)]
        max: usize,
    },
    /// Export a gzb file (or one wiki of it) as tab-separated text, with '#'
    /// metadata lines above the header.
    #[command(name = "gzb_tsv")]
    GzbTsv {
        group: GroupId,
        #[command(flatten)]
        month: MonthArg,
        /// Export only this wiki, e.g. enwiki.
        wiki: Option<Dbname>,
        /// Write here instead of to stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Add new wikis to the sites table and fill in missing language names.
    /// gzb_month does this too.
    #[command(name = "update_sites")]
    UpdateSites,
}

#[derive(Debug, Args)]
struct MonthArg {
    /// Year, or "lm" for last month's.
    #[arg(value_parser = parse_year)]
    year: i32,
    /// Month (1-12), or "lm" for last month.
    #[arg(value_parser = parse_month)]
    month: u32,
}

impl MonthArg {
    fn year_month(&self) -> Result<YearMonth> {
        YearMonth::new(self.year, self.month)
    }
}

#[derive(Debug, Args)]
struct MonthFlags {
    /// Pageview dump to use instead of the one on Toolforge's dumps mount.
    #[arg(long)]
    dump: Option<PathBuf>,
    /// Only these groups (active or not), e.g. --groups=1,2.
    #[arg(long, value_delimiter = ',')]
    groups: Option<Vec<GroupId>>,
    /// Regenerate groups that are already complete.
    #[arg(long)]
    force: bool,
    /// Groups listed at once in phase 1 (replica-bound).
    #[arg(long, default_value_t = 6)]
    list_jobs: usize,
    /// Group files built at once in phase 3 (CPU-bound).
    #[arg(long, default_value_t = 3)]
    build_jobs: usize,
    /// Keep page lists and views.bin after a fully successful run.
    #[arg(long)]
    keep_work: bool,
    /// Skip the gzb_check preflight.
    #[arg(long)]
    no_check: bool,
}

fn last_month() -> DateTime<Utc> {
    Utc::now()
        .checked_sub_months(Months::new(1))
        .expect("current date minus one month")
}

fn parse_year(s: &str) -> Result<i32, String> {
    match s {
        "lm" => Ok(last_month().year()),
        _ => s
            .parse()
            .map_err(|_| format!("year or 'lm' expected, not '{s}'")),
    }
}

fn parse_month(s: &str) -> Result<u32, String> {
    match s {
        "lm" => Ok(last_month().month()),
        _ => match s.parse() {
            Ok(month @ 1..=12) => Ok(month),
            _ => Err(format!("month 1-12 or 'lm' expected, not '{s}'")),
        },
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Unconditional startup banner. If this line does not appear in the
    // captured output, the running binary is not this build, or output is
    // not being captured — before debugging logic, fix that. Includes the
    // build version so a stale deploy is obvious. To stderr when stdout
    // carries data (gzb_tsv without --out).
    let banner = format!(
        "baglama2 v{} starting — {:?}",
        env!("CARGO_PKG_VERSION"),
        cli.command
    );
    if matches!(cli.command, Command::GzbTsv { out: None, .. }) {
        eprintln!("{banner}");
    } else {
        println!("{banner}");
    }

    // Install a logger backend. Without this, all log::{info,warn,error,trace}
    // macros are silently discarded. Defaults to `info`; override per-module
    // via the RUST_LOG env var (e.g. `RUST_LOG=baglama2=trace`).
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = Config::load()?;
    // Reading a gzb file needs only the config: these work while the tool
    // DB is down.
    match cli.command {
        Command::GzbShow {
            group,
            month,
            wiki,
            max,
        } => {
            return show(
                &config,
                group,
                &month.year_month()?,
                wiki.as_ref().map(Dbname::as_str),
                max,
            )
        }
        Command::GzbTsv {
            group,
            month,
            wiki,
            out,
        } => return tsv(&config, group, &month.year_month()?, wiki, out.as_deref()).await,
        _ => {}
    }

    info!("Starting up; initializing Baglama2 (config + DB pool + Wikidata API)");
    let baglama = Arc::new(with_timeout("Baglama2::new", 600, Baglama2::new(config)).await?);
    match cli.command {
        Command::GzbCheck { month, dump } => {
            let opts = gzb::month::MonthOptions {
                dump_override: dump,
                ..Default::default()
            };
            let report = gzb::month::GzbMonth::new(baglama, month.year_month()?, opts)
                .check()
                .await?;
            if !report.problems.is_empty() {
                return Err(anyhow!("{} problem(s)", report.problems.len()));
            }
            Ok(())
        }
        Command::GzbMonth { month, flags } => {
            let opts = gzb::month::MonthOptions {
                dump_override: flags.dump,
                group_ids: flags.groups,
                force: flags.force,
                list_jobs: flags.list_jobs,
                build_jobs: flags.build_jobs,
                keep_work: flags.keep_work,
                no_check: flags.no_check,
            };
            let job = gzb::month::GzbMonth::new(baglama.clone(), month.year_month()?, opts);
            // As before every monthly run.
            with_timeout(
                "deactivate_nonexistent_categories",
                600,
                baglama.deactivate_nonexistent_categories(),
            )
            .await?;
            job.run().await
        }
        Command::UpdateSites => with_timeout("update_sites", 600, baglama.update_sites()).await,
        Command::GzbShow { .. } | Command::GzbTsv { .. } => unreachable!("handled above"),
    }
}

fn show(
    config: &Config,
    group: GroupId,
    ym: &YearMonth,
    wiki: Option<&str>,
    max: usize,
) -> Result<()> {
    let path = gzb::gzb_path(&config.gzb_data_root_path, group, ym);
    let mut reader = gzb::GzbReader::open(&path)?;
    let h = reader.header().clone();
    match wiki {
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

async fn tsv(
    config: &Config,
    group: GroupId,
    ym: &YearMonth,
    wiki: Option<Dbname>,
    out: Option<&Path>,
) -> Result<()> {
    let path = gzb::gzb_path(&config.gzb_data_root_path, group, ym);
    let mut reader = gzb::GzbReader::open(&path)?;
    let meta = gzb::tsv::TsvMeta {
        group_label: group_label(config, group).await,
        wiki: wiki.map(|wiki| wiki.to_string()),
    };
    let rows = match out {
        Some(out) => {
            let file = std::fs::File::create(out)?;
            let rows = gzb::tsv::export(&mut reader, &meta, &mut std::io::BufWriter::new(file))?;
            println!("{rows} rows written to {}", out.display());
            rows
        }
        None => gzb::tsv::export(&mut reader, &meta, &mut std::io::stdout().lock())?,
    };
    info!("gzb_tsv: {rows} rows");
    Ok(())
}

/// What the group tracks, for the TSV comments. Best-effort: without the
/// tool DB, the export goes ahead without it.
async fn group_label(config: &Config, group: GroupId) -> Option<String> {
    let load = async {
        let db = db::Db::new(config)?;
        RowGroup::load(&db, group).await
    };
    let group = match tokio::time::timeout(LABEL_TIMEOUT, load).await {
        Ok(Ok(Some(group))) => group,
        Ok(Ok(None)) => return None,
        Ok(Err(e)) => {
            warn!("no group label, tool DB failed: {e:#}");
            return None;
        }
        Err(_) => {
            warn!(
                "no group label, tool DB did not answer within {}s",
                LABEL_TIMEOUT.as_secs()
            );
            return None;
        }
    };
    Some(group.label())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Command {
        Cli::try_parse_from(std::iter::once("baglama2").chain(args.iter().copied()))
            .unwrap()
            .command
    }

    #[test]
    fn test_cli_definition() {
        Cli::command().debug_assert();
    }

    #[test]
    fn test_gzb_month_flags() {
        let Command::GzbMonth { month, flags } = parse(&[
            "gzb_month",
            "2026",
            "9",
            "--groups=1,2",
            "--force",
            "--dump",
            "/x.bz2",
            "--build-jobs=2",
            "--no-check",
        ]) else {
            panic!("not gzb_month");
        };
        assert_eq!((month.year, month.month), (2026, 9));
        assert_eq!(
            flags.groups,
            Some(vec![GroupId::new(1).unwrap(), GroupId::new(2).unwrap()])
        );
        assert!(flags.force && flags.no_check && !flags.keep_work);
        assert_eq!(flags.dump, Some(PathBuf::from("/x.bz2")));
        assert_eq!((flags.list_jobs, flags.build_jobs), (6, 2));
    }

    #[test]
    fn test_dump_equals_form() {
        let Command::GzbCheck { dump, .. } = parse(&["gzb_check", "2026", "5", "--dump=/d.bz2"])
        else {
            panic!("not gzb_check");
        };
        assert_eq!(dump, Some(PathBuf::from("/d.bz2")));
    }

    #[test]
    fn test_show_positionals() {
        let Command::GzbShow {
            group,
            month,
            wiki,
            max,
        } = parse(&["gzb_show", "979", "2026", "9", "enwiki", "--max=5"])
        else {
            panic!("not gzb_show");
        };
        assert_eq!((group.get(), month.year, month.month), (979, 2026, 9));
        assert_eq!(wiki.as_ref().map(Dbname::as_str), Some("enwiki"));
        assert_eq!(max, 5);
    }

    #[test]
    fn test_tsv_without_wiki() {
        let Command::GzbTsv { wiki, out, .. } =
            parse(&["gzb_tsv", "979", "2026", "9", "--out", "/tmp/x.tsv"])
        else {
            panic!("not gzb_tsv");
        };
        assert_eq!(wiki, None);
        assert_eq!(out, Some(PathBuf::from("/tmp/x.tsv")));
    }

    #[test]
    fn test_parse_year_month() {
        assert_eq!(parse_year("2023"), Ok(2023));
        assert_eq!(parse_year("lm"), Ok(last_month().year()));
        assert!(parse_year("bar").is_err());
        assert_eq!(parse_month("3"), Ok(3));
        assert_eq!(parse_month("12"), Ok(12));
        assert_eq!(parse_month("lm"), Ok(last_month().month()));
        assert!(parse_month("13").is_err());
        assert!(parse_month("0").is_err());
        assert!(parse_month("foo").is_err());
    }

    #[test]
    fn test_unknown_command_and_missing_month() {
        assert!(Cli::try_parse_from(["baglama2", "mysql2_views", "2026", "5"]).is_err());
        assert!(Cli::try_parse_from(["baglama2", "gzb_show", "0", "2026", "5"]).is_err());
        assert!(
            Cli::try_parse_from(["baglama2", "gzb_show", "1", "2026", "5", "en.wikipedia"])
                .is_err()
        );
        assert!(
            Cli::try_parse_from(["baglama2", "gzb_month", "2026", "5", "--groups=1,0"]).is_err()
        );
        assert!(Cli::try_parse_from(["baglama2", "gzb_month", "2026"]).is_err());
    }
}
