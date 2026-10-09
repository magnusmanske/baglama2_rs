//! `gzb_month`: generate one month of view data for all active groups.
//!
//! Three phases, each resumable after a crash or timeout:
//!
//! 1. **Page lists** (replicas). Per group: the files in its category tree
//!    (or uploaded by the user), then their global usage, written to
//!    `<root>/work/<YYYYMM>/<gid>.tsv.gz` as
//!    `giu \t namespace_id \t dump_title \t file`. Status `SCANNED`.
//! 2. **Views** (one pass over the pageview dump). Every page from every
//!    page list becomes a 64-bit key in one in-memory table; scanning the
//!    dump fills in the counts. Saved as `views.bin` next to the page lists.
//! 3. **Files**. Per group: page list + views → `<root>/<YYYYMM>/<gid>.gzb`.
//!    Status `VIEW DATA COMPLETE`, storage `gzb`.
//!
//! Group progress is logged in `group_status` as the other pipelines do.

use super::*;
use crate::baglama2::IN_CHUNK;
use crate::global_image_links::GlobalImageLinks;
use crate::group_status::{self, GroupStatus};
use crate::pageviews::dump_reader;
use crate::row_group::{GroupSource, RowGroup};
use crate::wiki::{Dbname, DumpCode};
use crate::{Baglama2, GroupId};
use log::{error, info, warn};
use mysql_async::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;

/// Files per `globalimagelinks` query, as in the older pipelines.
const GIL_CHUNK: usize = 3000;

/// Page-list size (compressed) that counts as one unit of build concurrency;
/// keeps several huge groups from being held in memory at once.
const BUILD_WEIGHT_BYTES: u64 = 16 * 1024 * 1024;

/// Smallest plausible monthly `-user` dump; anything below is truncated.
const MIN_DUMP_BYTES: u64 = 1024 * 1024 * 1024;

const VIEWS_BIN_MAGIC: &[u8; 10] = b"BGZVIEWS1\n";

#[derive(Debug, Clone)]
pub struct MonthOptions {
    pub dump_override: Option<PathBuf>,
    /// Only these groups (active or not); default all active groups.
    pub group_ids: Option<Vec<GroupId>>,
    /// Regenerate even groups that already have complete data.
    pub force: bool,
    /// Concurrent groups in phase 1 (replica-bound).
    pub list_jobs: usize,
    /// Concurrent groups in phase 3 (CPU-bound).
    pub build_jobs: usize,
    /// Keep page lists and `views.bin` after a fully successful run.
    pub keep_work: bool,
    /// Skip the preflight [`GzbMonth::check`] that `run` does first.
    pub no_check: bool,
}

impl Default for MonthOptions {
    fn default() -> Self {
        Self {
            dump_override: None,
            group_ids: None,
            force: false,
            list_jobs: 6,
            build_jobs: 3,
            keep_work: false,
            no_check: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    Skip(&'static str),
    ListPages,
    BuildFile,
}

/// What to do with one group, given its `group_status` status for the month.
pub fn plan_group(status: Option<GroupStatus>, work_file_exists: bool, force: bool) -> Plan {
    if force {
        return Plan::ListPages;
    }
    match status {
        Some(GroupStatus::Complete) => Plan::Skip("already complete"),
        Some(GroupStatus::Scanned) if work_file_exists => Plan::BuildFile,
        _ => Plan::ListPages,
    }
}

#[derive(Debug, Default)]
pub struct CheckReport {
    pub problems: Vec<String>,
    pub dump: Option<PathBuf>,
}

pub struct GzbMonth {
    baglama: Arc<Baglama2>,
    ym: YearMonth,
    opts: MonthOptions,
    root: PathBuf,
    work: PathBuf,
}

impl GzbMonth {
    pub fn new(baglama: Arc<Baglama2>, ym: YearMonth, opts: MonthOptions) -> Self {
        let root = baglama.config().gzb_data_root_path.clone();
        let work = root.join("work").join(year_month_dir(&ym));
        Self {
            baglama,
            ym,
            opts,
            root,
            work,
        }
    }

    fn work_file(&self, group_id: GroupId) -> PathBuf {
        self.work.join(format!("{group_id}.tsv.gz"))
    }

    fn views_bin(&self) -> PathBuf {
        self.work.join("views.bin")
    }

    fn expected_dump_path(&self) -> PathBuf {
        match &self.opts.dump_override {
            Some(path) => path.clone(),
            None => dump_reader::local_dump_path_unchecked(self.ym.year(), self.ym.month()),
        }
    }

    /// Everything that would make a run fail early, checked up front.
    /// Problems are returned, not raised, so all of them get reported.
    pub async fn check(&self) -> Result<CheckReport> {
        let mut report = CheckReport::default();
        let ym = &self.ym;
        println!("== gzb check for {ym}");

        let now = chrono::Utc::now();
        use chrono::Datelike;
        if (ym.year(), ym.month()) >= (now.year(), now.month()) {
            report.problems.push(format!(
                "{ym} is not over yet; its pageview dump cannot exist"
            ));
        }

        let dump = self.expected_dump_path();
        match std::fs::metadata(&dump) {
            Ok(meta) if meta.is_file() && meta.len() >= MIN_DUMP_BYTES => {
                println!(
                    "dump: {} ({:.1} GB)",
                    dump.display(),
                    meta.len() as f64 / 1e9
                );
                report.dump = Some(dump);
            }
            Ok(meta) => report.problems.push(format!(
                "dump {} is only {} bytes; truncated or still being written?",
                dump.display(),
                meta.len()
            )),
            Err(e) => report.problems.push(format!(
                "dump {} not readable ({e}); it is usually published in the first days of the \
                 following month. On Toolforge, run the job with --mount=all.",
                dump.display()
            )),
        }

        for dir in [self.root.join(year_month_dir(ym)), self.work.clone()] {
            let probe = dir.join(".write_probe");
            let ok = std::fs::create_dir_all(&dir)
                .and_then(|_| std::fs::write(&probe, b"ok"))
                .and_then(|_| std::fs::remove_file(&probe));
            match ok {
                Ok(()) => println!("writable: {}", dir.display()),
                Err(e) => report
                    .problems
                    .push(format!("cannot write to {}: {e}", dir.display())),
            }
        }

        match self.check_tooldb().await {
            Ok(()) => {}
            Err(e) => report.problems.push(format!("tool DB: {e}")),
        }

        for tables in [
            &["page", "categorylinks", "linktarget"][..],
            &["globalimagelinks"][..],
            &["image", "actor", "user"][..],
        ] {
            let res = async {
                let mut conn = self
                    .baglama
                    .db()
                    .get_commons_conn_for_tables(tables)
                    .await?;
                // Long queries rely on the server ending them; see `query_commons`.
                conn.query_drop("SET SESSION max_statement_time=600")
                    .await?;
                conn.query_drop("SELECT 1").await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            match res {
                Ok(()) => println!("replica for {tables:?}: OK"),
                Err(e) => report.problems.push(format!("replica for {tables:?}: {e}")),
            }
        }

        let sites = self.baglama.get_sites()?;
        let missing: Vec<String> = sites
            .iter()
            .filter_map(|s| s.giu_code())
            .filter(|giu| self.baglama.wiki_dump_code(giu).is_none())
            .map(|giu| giu.to_string())
            .collect();
        println!(
            "dump codes: {} of {} wikis resolved{}",
            sites.len() - missing.len(),
            sites.len(),
            if missing.is_empty() {
                String::new()
            } else {
                format!(
                    "; unresolved (views will be 0): {}",
                    missing
                        .iter()
                        .take(20)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        );

        if report.problems.is_empty() {
            println!("== OK, ready to generate {ym}");
        } else {
            for p in &report.problems {
                println!("PROBLEM: {p}");
            }
        }
        Ok(report)
    }

    async fn check_tooldb(&self) -> Result<()> {
        let active: Option<u64> = self
            .baglama
            .db()
            .get_tooldb_conn()
            .await?
            .query_first("SELECT COUNT(*) FROM `groups` WHERE is_active=1")
            .await?;
        println!("tool DB: OK, {} active groups", active.unwrap_or(0));
        for (status, n) in group_status::counts(self.baglama.db(), &self.ym).await? {
            println!("  existing for {}: {n} × {status}", self.ym);
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let dump = if self.opts.no_check {
            // Unverified: a missing dump only surfaces in phase 2, after the
            // page lists are saved, so a re-run resumes from there.
            info!("--no-check: skipping the preflight check");
            self.expected_dump_path()
        } else {
            let report = self.check().await?;
            if !report.problems.is_empty() {
                return Err(anyhow!(
                    "{} problem(s) found, not starting; see above",
                    report.problems.len()
                ));
            }
            report
                .dump
                .ok_or_else(|| anyhow!("no dump despite a clean check"))?
        };
        // The check creates these too, but --no-check must not leave every
        // group failing on a missing directory.
        for dir in [self.root.join(year_month_dir(&self.ym)), self.work.clone()] {
            std::fs::create_dir_all(&dir)
                .map_err(|e| anyhow!("cannot create {}: {e}", dir.display()))?;
        }
        self.baglama.update_sites().await?;

        let plans = self.select_groups().await?;
        let ids_with = |plan: Plan| -> Vec<GroupId> {
            plans
                .iter()
                .filter(|(_, p)| *p == plan)
                .map(|(id, _)| *id)
                .collect()
        };
        let to_list = ids_with(Plan::ListPages);
        let listed_before = ids_with(Plan::BuildFile);
        info!(
            "{}: {} groups to list, {} already listed, {} skipped",
            self.ym,
            to_list.len(),
            listed_before.len(),
            plans.len() - to_list.len() - listed_before.len()
        );

        let memory_log = log_memory_periodically();
        let mut listed = self.phase_list_pages(&to_list, self.opts.list_jobs).await;
        // Failures here are mostly the replicas being overloaded, often by the
        // concurrency itself. One more pass, a group at a time, costs little
        // next to re-running the month (which rescans the whole dump).
        let retry: Vec<GroupId> = {
            let ok: HashSet<GroupId> = listed.iter().copied().collect();
            to_list
                .iter()
                .copied()
                .filter(|id| !ok.contains(id))
                .collect()
        };
        if !retry.is_empty() {
            info!(
                "Phase 1: retrying {} failed groups one at a time",
                retry.len()
            );
            listed.extend(self.phase_list_pages(&retry, 1).await);
        }
        let list_failed = to_list.len() - listed.len();
        let mut build: Vec<GroupId> = listed_before.into_iter().chain(listed).collect();
        build.sort();
        if build.is_empty() {
            info!("Nothing to build for {}", self.ym);
            return Ok(());
        }

        let views = Arc::new(self.phase_views(&build, &dump).await?);
        let build_failed = self.phase_build(&build, views).await;
        memory_log.abort();

        info!(
            "{}: {} group files written; {list_failed} failed listing pages, {build_failed} failed writing",
            self.ym,
            build.len() - build_failed
        );
        let failed = list_failed + build_failed;
        if failed > 0 {
            return Err(anyhow!(
                "{failed} groups failed; re-run the same command to retry them"
            ));
        }
        // Keep the work dir after a partial (--groups) run: views.bin is only
        // valid for the page lists it was built from, and is cheap to keep.
        if !self.opts.keep_work && self.opts.group_ids.is_none() {
            info!("All groups done; removing {}", self.work.display());
            let _ = std::fs::remove_dir_all(&self.work);
        }
        Ok(())
    }

    async fn select_groups(&self) -> Result<Vec<(GroupId, Plan)>> {
        let rows = group_status::groups_for_month(self.baglama.db(), &self.ym).await?;
        let wanted: Option<HashSet<GroupId>> = self
            .opts
            .group_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect());
        let mut ret = vec![];
        for (id, is_active, status) in rows {
            let selected = match &wanted {
                Some(ids) => ids.contains(&id),
                None => is_active,
            };
            if !selected {
                continue;
            }
            let plan = plan_group(status, self.work_file(id).is_file(), self.opts.force);
            if let Plan::Skip(why) = plan {
                info!("group {id}: skipped, {why}");
            }
            ret.push((id, plan));
        }
        if let Some(ids) = &wanted {
            let found: HashSet<GroupId> = ret.iter().map(|(id, _)| *id).collect();
            for id in ids.difference(&found) {
                warn!("group {id} does not exist");
            }
        }
        Ok(ret)
    }

    // ------------------------------------------------------------------
    // Phase 1: page lists
    // ------------------------------------------------------------------

    /// Lists pages for `group_ids`, `jobs` groups at a time. Returns the
    /// groups whose page list was written.
    async fn phase_list_pages(&self, group_ids: &[GroupId], jobs: usize) -> Vec<GroupId> {
        if group_ids.is_empty() {
            return vec![];
        }
        info!("Phase 1: listing pages for {} groups", group_ids.len());
        let semaphore = Arc::new(Semaphore::new(jobs.max(1)));
        let mut join_set = tokio::task::JoinSet::new();
        for (n, &group_id) in group_ids.iter().enumerate() {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            let baglama = self.baglama.clone();
            let ym = self.ym;
            let tmp = self.work.join(format!("{group_id}.tsv.gz.tmp"));
            let out = self.work_file(group_id);
            info!("Phase 1: group {group_id} ({}/{})", n + 1, group_ids.len());
            join_set.spawn(async move {
                let res = list_pages(&baglama, &ym, group_id, &tmp, &out).await;
                drop(permit);
                match res {
                    Ok(rows) => {
                        info!("Phase 1: group {group_id} listed, {rows} usages");
                        Some(group_id)
                    }
                    Err(e) => {
                        error!("Phase 1: group {group_id} failed: {e:#}");
                        let _ = std::fs::remove_file(&tmp);
                        let _ = std::fs::remove_file(names_file(&tmp));
                        let _ = group_status::set(
                            baglama.db(),
                            group_id,
                            &ym,
                            GroupStatus::Failed,
                            None,
                        )
                        .await;
                        None
                    }
                }
            });
        }
        let mut done = vec![];
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Some(id)) => done.push(id),
                Ok(None) => {}
                Err(e) => error!("Phase 1 task panicked: {e}"),
            }
        }
        done
    }

    // ------------------------------------------------------------------
    // Phase 2: views from the dump
    // ------------------------------------------------------------------

    async fn phase_views(&self, group_ids: &[GroupId], dump: &Path) -> Result<ViewTable> {
        let views_bin = self.views_bin();
        let work_files: Vec<PathBuf> = group_ids.iter().map(|id| self.work_file(*id)).collect();
        if views_bin_is_fresh(&views_bin, &work_files) {
            info!("Phase 2: reusing {}", views_bin.display());
            return tokio::task::spawn_blocking(move || read_views_bin(&views_bin)).await?;
        }
        let codes = DumpCodes::new(self.baglama.clone())?;
        let dump = dump.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut codes = codes;
            let (mut views, needed) = collect_page_keys(&work_files, &mut codes)?;
            info!(
                "Phase 2: {} distinct pages on {} wikis; scanning {}",
                views.len(),
                needed.len(),
                dump.display()
            );
            scan_dump_into(&dump, &needed, &mut views)?;
            let with_views = views.with_views().count();
            let total: u64 = views.with_views().map(|(_, v)| v as u64).sum();
            info!(
                "Phase 2: {with_views} of {} pages have views, {total} views in total",
                views.len()
            );
            write_views_bin(&views_bin, &views)?;
            Ok(views)
        })
        .await?
    }

    // ------------------------------------------------------------------
    // Phase 3: gzb files
    // ------------------------------------------------------------------

    /// Returns the number of failed groups.
    async fn phase_build(&self, group_ids: &[GroupId], views: Arc<ViewTable>) -> usize {
        info!("Phase 3: writing {} group files", group_ids.len());
        let jobs = self.opts.build_jobs.max(1);
        let semaphore = Arc::new(Semaphore::new(jobs));
        let codes = match DumpCodes::new(self.baglama.clone()) {
            Ok(codes) => codes,
            Err(e) => {
                error!("Phase 3: {e}");
                return group_ids.len();
            }
        };
        let mut join_set = tokio::task::JoinSet::new();
        for &group_id in group_ids {
            let work_file = self.work_file(group_id);
            let size = std::fs::metadata(&work_file).map(|m| m.len()).unwrap_or(0);
            let weight = (size / BUILD_WEIGHT_BYTES).clamp(1, jobs as u64) as u32;
            let permit = semaphore
                .clone()
                .acquire_many_owned(weight)
                .await
                .expect("semaphore closed");
            let baglama = self.baglama.clone();
            let ym = self.ym;
            let out = gzb_path(&self.root, group_id, &ym);
            let views = views.clone();
            let mut codes = codes.clone();
            join_set.spawn(async move {
                let res = tokio::task::spawn_blocking(move || {
                    build_group_file(group_id, &ym, &work_file, &out, &views, &mut codes)
                })
                .await
                .map_err(anyhow::Error::from)
                .and_then(|r| r);
                drop(permit);
                match res {
                    Ok(header) => {
                        info!(
                            "Phase 3: group {group_id}: {} pages on {} wikis, {} views",
                            header.total_pages,
                            header.sites.len(),
                            header.total_views
                        );
                        let total = Some(db_views(header.total_views));
                        match group_status::set(
                            baglama.db(),
                            group_id,
                            &ym,
                            GroupStatus::Complete,
                            total,
                        )
                        .await
                        {
                            Ok(()) => true,
                            Err(e) => {
                                error!("Phase 3: group {group_id}: status update failed: {e}");
                                false
                            }
                        }
                    }
                    Err(e) => {
                        error!("Phase 3: group {group_id} failed: {e:#}");
                        let _ = group_status::set(
                            baglama.db(),
                            group_id,
                            &ym,
                            GroupStatus::Failed,
                            None,
                        )
                        .await;
                        false
                    }
                }
            });
        }
        let mut failed = 0;
        while let Some(res) = join_set.join_next().await {
            if !matches!(res, Ok(true)) {
                failed += 1;
            }
        }
        failed
    }
}

async fn list_pages(
    baglama: &Baglama2,
    ym: &YearMonth,
    group_id: GroupId,
    tmp: &Path,
    out: &Path,
) -> Result<u64> {
    group_status::set(baglama.db(), group_id, ym, GroupStatus::Listing, None).await?;
    let group = RowGroup::load(baglama.db(), group_id)
        .await?
        .ok_or_else(|| anyhow!("group {group_id} not found"))?;
    let mut enc = GzEncoder::new(
        std::io::BufWriter::new(File::create(tmp)?),
        Compression::fast(),
    );
    let mut rows = 0u64;
    let mut batches = FileBatches::default();
    // File names stream to a temp file, then into the usage queries a batch
    // at a time, and usages stream into the page list row by row; what stays
    // in memory is the set of file names seen, not the lists of the largest
    // groups (4.8M files for "Uploaded with OpenRefine").
    match group.source() {
        GroupSource::Uploader(name) => {
            for file in baglama.get_files_from_user_name(name).await? {
                if let Some(batch) = batches.push(file) {
                    rows += write_usages(&mut enc, &batch, baglama).await?;
                }
            }
        }
        GroupSource::Category { title, depth } => {
            let categories = baglama.category_tree_of(title, *depth).await?;
            info!(
                "group {group_id} ({}): {} categories",
                group.label(),
                categories.len()
            );
            // The names go to disk first, as fast as the server sends them.
            // Running the usage queries between rows would stall the file
            // query, and the server drops a client that stops reading for
            // 60 s (`net_write_timeout`).
            let names = names_file(tmp);
            {
                let mut w = std::io::BufWriter::new(File::create(&names)?);
                for chunk in categories.chunks(IN_CHUNK) {
                    baglama
                        .for_each_file_in_categories(chunk, |file| {
                            writeln!(w, "{file}")?;
                            Ok(())
                        })
                        .await?;
                }
                w.flush()?;
            }
            for line in BufReader::new(File::open(&names)?).lines() {
                if let Some(batch) = batches.push(line?) {
                    rows += write_usages(&mut enc, &batch, baglama).await?;
                }
            }
            let _ = std::fs::remove_file(&names);
        }
    }
    let last = batches.finish();
    rows += write_usages(&mut enc, &last, baglama).await?;
    info!(
        "group {group_id} ({}): {} files, {rows} usages",
        group.label(),
        batches.seen()
    );
    enc.finish()?
        .into_inner()
        .map_err(|e| e.into_error())?
        .sync_all()?;
    std::fs::rename(tmp, out)?;
    group_status::set(baglama.db(), group_id, ym, GroupStatus::Scanned, None).await?;
    Ok(rows)
}

/// Where a group's file names wait between the category and usage queries.
fn names_file(tmp: &Path) -> PathBuf {
    tmp.with_extension("files.tmp")
}

/// Collects distinct file names into batches of [`GIL_CHUNK`] for the
/// `globalimagelinks` query. A file in several categories is seen more than
/// once and must be queried once.
#[derive(Default)]
struct FileBatches {
    seen: HashSet<String>,
    pending: Vec<String>,
}

impl FileBatches {
    /// Returns a full batch when `file` completes one.
    fn push(&mut self, file: String) -> Option<Vec<String>> {
        if !self.seen.insert(file.clone()) {
            return None;
        }
        self.pending.push(file);
        if self.pending.len() >= GIL_CHUNK {
            Some(std::mem::take(&mut self.pending))
        } else {
            None
        }
    }

    /// The last, partial batch.
    fn finish(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending)
    }

    fn seen(&self) -> usize {
        self.seen.len()
    }
}

/// Appends the usages of `files` to the page list; returns the rows written.
async fn write_usages<W: Write>(enc: &mut W, files: &[String], baglama: &Baglama2) -> Result<u64> {
    if files.is_empty() {
        return Ok(0);
    }
    let mut rows = 0;
    GlobalImageLinks::for_each(files, baglama.db(), |gil| {
        writeln!(
            enc,
            "{}\t{}\t{}\t{}",
            gil.wiki,
            gil.page_namespace_id,
            gil.dump_title(),
            gil.to
        )?;
        rows += 1;
        Ok(())
    })
    .await?;
    Ok(rows)
}

/// Resident memory of this process in MB, where `/proc` has it (Linux).
fn resident_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024)
}

/// Logs resident memory every minute until aborted, so a run that is
/// killed for exceeding its memory limit (no log line, no exit code on
/// Toolforge) at least shows the climb before it.
fn log_memory_periodically() -> tokio::task::AbortHandle {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Some(mb) = resident_mb() {
                info!("memory: {mb} MB resident");
            }
        }
    })
    .abort_handle()
}

/// Wiki database name (`enwiki`) → pageview dump code (`en.wikipedia`),
/// cached; `None` for wikis that have no dump code.
#[derive(Clone)]
pub struct DumpCodes {
    baglama: Option<Arc<Baglama2>>,
    known: HashMap<Dbname, Option<DumpCode>>,
}

impl DumpCodes {
    pub fn new(baglama: Arc<Baglama2>) -> Result<Self> {
        let known = baglama
            .get_sites()?
            .iter()
            .filter_map(|s| s.giu_code().cloned())
            .map(|giu| {
                let code = baglama.wiki_dump_code(&giu);
                (giu, code)
            })
            .collect();
        Ok(Self {
            baglama: Some(baglama),
            known,
        })
    }

    /// The dump code for `giu`, a database name as read from a page list.
    /// `None` if the wiki has none, or `giu` is not a database name.
    pub fn get(&mut self, giu: &str) -> Option<&DumpCode> {
        if !self.known.contains_key(giu) {
            let giu = Dbname::parse(giu).ok()?;
            let code = self.baglama.as_ref().and_then(|b| b.wiki_dump_code(&giu));
            self.known.insert(giu, code);
        }
        self.known.get(giu).and_then(Option::as_ref)
    }
}

/// One page-list line: `(giu, namespace_id, dump_title, file)`.
fn parse_work_line(line: &str) -> Option<(&str, i32, &str, &str)> {
    let mut cols = line.splitn(4, '\t');
    let giu = cols.next()?;
    let ns = cols.next()?.parse().ok()?;
    let title = cols.next()?;
    let file = cols.next()?;
    Some((giu, ns, title, file))
}

fn read_work_file(path: &Path) -> Result<impl Iterator<Item = std::io::Result<String>>> {
    Ok(BufReader::new(flate2::read::GzDecoder::new(File::open(path)?)).lines())
}

/// Every page in the page lists, as zero-view entries, plus the dump codes
/// worth looking at.
fn collect_page_keys(
    work_files: &[PathBuf],
    codes: &mut DumpCodes,
) -> Result<(ViewTable, HashSet<Vec<u8>>)> {
    // Pages used by several groups appear once per group; de-duplicating
    // whenever the list has doubled keeps it near the number of pages.
    const MIN_COMPACT: usize = 1 << 22;
    let mut keys: Vec<u64> = vec![];
    let mut compact_at = MIN_COMPACT;
    let mut needed = HashSet::new();
    let mut unknown: HashMap<String, u64> = HashMap::new();
    for (n, path) in work_files.iter().enumerate() {
        for line in read_work_file(path)? {
            let line = line?;
            let Some((giu, _ns, title, _file)) = parse_work_line(&line) else {
                continue;
            };
            match codes.get(giu) {
                Some(code) => {
                    keys.push(page_key(code.as_bytes(), title.as_bytes()));
                    if !needed.contains(code.as_bytes()) {
                        needed.insert(code.as_bytes().to_vec());
                    }
                }
                None => *unknown.entry(giu.to_string()).or_default() += 1,
            }
            if keys.len() >= compact_at {
                keys.sort_unstable();
                keys.dedup();
                compact_at = (keys.len() * 2).max(MIN_COMPACT);
            }
        }
        if (n + 1) % 100 == 0 {
            info!("Phase 2: read {}/{} page lists", n + 1, work_files.len());
        }
    }
    if !unknown.is_empty() {
        warn!("Usages on wikis without a dump code (views stay 0): {unknown:?}");
    }
    Ok((ViewTable::from_keys(keys), needed))
}

fn scan_dump_into(dump: &Path, needed: &HashSet<Vec<u8>>, views: &mut ViewTable) -> Result<()> {
    let mut last_code: Vec<u8> = vec![];
    let mut last_needed = false;
    let mut matched: u64 = 0;
    let file = std::io::BufReader::with_capacity(4 * 1024 * 1024, File::open(dump)?);
    dump_reader::scan_dump_lines(file, |code, title, n| {
        if code != last_code.as_slice() {
            last_code = code.to_vec();
            last_needed = needed.contains(code);
        }
        // The legacy pipeline hid the Main Page too; it is not a GLAM's doing.
        if !last_needed || title == b"Main_Page" {
            return;
        }
        if views.add(page_key(code, title), n) {
            matched += 1;
        }
    })?;
    info!("Phase 2: {matched} matching dump lines");
    Ok(())
}

fn views_bin_is_fresh(views_bin: &Path, work_files: &[PathBuf]) -> bool {
    let mtime = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
    let Some(bin_time) = mtime(views_bin) else {
        return false;
    };
    work_files
        .iter()
        .all(|f| mtime(f).is_some_and(|t: SystemTime| t <= bin_time))
}

/// `views.bin`: magic, entry count, then `(u64 key, u32 views)` little-endian
/// for every page with views. Pages without views are implied.
fn write_views_bin(path: &Path, views: &ViewTable) -> Result<()> {
    let tmp = path.with_extension("bin.tmp");
    {
        let mut f = std::io::BufWriter::new(File::create(&tmp)?);
        f.write_all(VIEWS_BIN_MAGIC)?;
        let n = views.with_views().count() as u64;
        f.write_all(&n.to_le_bytes())?;
        for (k, v) in views.with_views() {
            f.write_all(&k.to_le_bytes())?;
            f.write_all(&v.to_le_bytes())?;
        }
        f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn read_views_bin(path: &Path) -> Result<ViewTable> {
    let mut f = BufReader::new(File::open(path)?);
    let mut magic = [0u8; 10];
    f.read_exact(&mut magic)?;
    if &magic != VIEWS_BIN_MAGIC {
        return Err(anyhow!("{} is not a views table", path.display()));
    }
    let mut n = [0u8; 8];
    f.read_exact(&mut n)?;
    let n = u64::from_le_bytes(n) as usize;
    let mut pairs = Vec::with_capacity(n);
    let mut entry = [0u8; 12];
    for _ in 0..n {
        f.read_exact(&mut entry)?;
        let key = u64::from_le_bytes(entry[0..8].try_into()?);
        let v = u32::from_le_bytes(entry[8..12].try_into()?);
        pairs.push((key, v));
    }
    Ok(ViewTable::from_pairs(pairs))
}

/// One line of a page list, compactly: the title is a slice of a shared
/// byte buffer, wiki and file are indexes into interned lists. 20 bytes plus
/// the title, where a per-page map of owned strings took ~250 bytes — which
/// for a 7M-page group was the difference between ~0.5 and ~2.5 GB.
struct Usage {
    title_start: u32,
    title_len: u32,
    file: u32,
    namespace_id: i32,
    wiki: u16,
}

/// A page: `count` consecutive usages starting at `first`, after sorting.
struct Page {
    first: u32,
    count: u32,
    views: u64,
}

fn build_group_file(
    group_id: GroupId,
    ym: &YearMonth,
    work_file: &Path,
    out: &Path,
    views: &ViewTable,
    codes: &mut DumpCodes,
) -> Result<GzbHeader> {
    let mut wiki_ids: HashMap<String, u16> = HashMap::new();
    let mut file_ids: HashMap<String, u32> = HashMap::new();
    let mut titles: Vec<u8> = vec![];
    let mut usages: Vec<Usage> = vec![];
    for line in read_work_file(work_file)? {
        let line = line?;
        let Some((giu, namespace_id, title, file)) = parse_work_line(&line) else {
            continue;
        };
        let title_start = u32::try_from(titles.len())
            .map_err(|_| anyhow!("group {group_id}: over 4 GB of titles"))?;
        titles.extend_from_slice(title.as_bytes());
        usages.push(Usage {
            title_start,
            title_len: title.len() as u32,
            file: intern(&mut file_ids, file)?,
            namespace_id,
            wiki: intern(&mut wiki_ids, giu)?,
        });
    }
    let wikis = interned_list(wiki_ids);
    let files = interned_list(file_ids);
    let title = |u: &Usage| &titles[u.title_start as usize..(u.title_start + u.title_len) as usize];

    // Groups each page's usages, wikis in turn, titles ascending.
    usages.sort_unstable_by(|a, b| {
        a.wiki
            .cmp(&b.wiki)
            .then_with(|| title(a).cmp(title(b)))
            .then(a.file.cmp(&b.file))
    });

    let mut writer = GzbWriter::new(out, group_id, ym, "dump");
    let mut names: Vec<&str> = vec![];
    let mut start = 0;
    while start < usages.len() {
        let wiki = usages[start].wiki;
        let end = usages[start..]
            .iter()
            .position(|u| u.wiki != wiki)
            .map_or(usages.len(), |p| start + p);
        let giu = &wikis[wiki as usize];
        let code = codes.get(giu).map(|c| c.as_bytes().to_vec());

        let mut pages: Vec<Page> = vec![];
        let mut i = start;
        while i < end {
            let t = title(&usages[i]);
            let j = usages[i..end]
                .iter()
                .position(|u| title(u) != t)
                .map_or(end, |p| i + p);
            let views = code
                .as_ref()
                .map_or(0, |c| views.get(page_key(c, t)) as u64);
            pages.push(Page {
                first: i as u32,
                count: (j - i) as u32,
                views,
            });
            i = j;
        }
        // Stable: equal views keep their ascending title order.
        pages.sort_by_key(|p| std::cmp::Reverse(p.views));
        let total_views = pages.iter().map(|p| p.views).sum();

        writer.add_site_sorted(
            giu,
            pages.len(),
            pages.len() as u64,
            total_views,
            |n, buf| {
                let page = &pages[n];
                let page_usages = &usages[page.first as usize..(page.first + page.count) as usize];
                names.clear();
                names.extend(page_usages.iter().map(|u| files[u.file as usize].as_str()));
                names.sort_unstable();
                names.dedup();
                // Titles were &str when stored, so this cannot fail.
                let t = std::str::from_utf8(title(&page_usages[0])).unwrap_or_default();
                write_row(
                    buf,
                    t,
                    page_usages[0].namespace_id,
                    page.views,
                    names.iter().copied(),
                );
            },
        )?;
        start = end;
    }
    writer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_group() {
        assert_eq!(plan_group(None, false, false), Plan::ListPages);
        assert_eq!(
            plan_group(Some(GroupStatus::Complete), false, false),
            Plan::Skip("already complete")
        );
        assert_eq!(
            plan_group(Some(GroupStatus::Scanned), true, false),
            Plan::BuildFile
        );
        // A SCANNED row without its page list (deleted work dir) is redone.
        assert_eq!(
            plan_group(Some(GroupStatus::Scanned), false, false),
            Plan::ListPages
        );
        assert_eq!(
            plan_group(Some(GroupStatus::Failed), true, false),
            Plan::ListPages
        );
        assert_eq!(
            plan_group(Some(GroupStatus::Complete), true, true),
            Plan::ListPages
        );
    }

    #[test]
    fn test_file_batches() {
        let mut batches = FileBatches::default();
        let mut full = vec![];
        for i in 0..(GIL_CHUNK * 2 + 5) {
            // Every file twice: the repeat must not count.
            for _ in 0..2 {
                if let Some(batch) = batches.push(format!("F{i}.jpg")) {
                    full.push(batch);
                }
            }
        }
        assert_eq!(full.len(), 2);
        assert!(full.iter().all(|b| b.len() == GIL_CHUNK));
        assert_eq!(full[0][0], "F0.jpg");
        assert_eq!(batches.finish().len(), 5);
        assert!(batches.finish().is_empty());
        assert_eq!(batches.seen(), GIL_CHUNK * 2 + 5);
    }

    #[test]
    fn test_parse_work_line() {
        assert_eq!(
            parse_work_line("dewiki\t14\tKategorie:Köln\tA.jpg"),
            Some(("dewiki", 14, "Kategorie:Köln", "A.jpg"))
        );
        assert_eq!(parse_work_line("dewiki\tx\tFoo\tA.jpg"), None);
        assert_eq!(parse_work_line("dewiki\t0\tFoo"), None);
    }

    #[test]
    fn test_views_bin_roundtrip() {
        let path = std::env::temp_dir().join(format!("views_{}.bin", std::process::id()));
        let a = page_key(b"en.wikipedia", b"A");
        let b = page_key(b"en.wikipedia", b"B");
        let mut views = ViewTable::from_keys(vec![a, b, u64::MAX]);
        views.add(a, 5);
        views.add(u64::MAX, u64::MAX);
        write_views_bin(&path, &views).unwrap();
        let back = read_views_bin(&path).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back.get(a), 5);
        assert_eq!(back.get(b), 0);
        assert_eq!(back.get(u64::MAX), u32::MAX);
        std::fs::remove_file(&path).unwrap();
    }

    /// Page keys (phase 2, without the dump scan) and the file build (phase 3)
    /// for a synthetic group the size of group 979, the largest: 7.3M pages.
    /// For allocator and memory comparisons; measure peak RSS from outside:
    /// `/usr/bin/time -l cargo test --release bench_large_group -- --ignored --nocapture`
    /// `BENCH_PAGES` sets the size.
    #[test]
    #[ignore]
    fn bench_large_group() {
        use std::time::Instant;
        let pages: usize = std::env::var("BENCH_PAGES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7_300_000);
        let dir = std::env::temp_dir().join(format!("gzb_bench_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Skewed like real usage: half the pages on one wiki, a quarter on
        // the next, and so on.
        let wikis: Vec<(String, String)> = (0..50)
            .map(|w| (format!("w{w}wiki"), format!("w{w}.wikipedia")))
            .collect();
        let page = |i: usize| -> (&(String, String), String) {
            let w = ((i + 1).trailing_zeros() as usize).min(wikis.len() - 1);
            (&wikis[w], format!("Page_{i:x}_Zürich_{}", i % 997))
        };

        let t = Instant::now();
        let work = dir.join("979.tsv.gz");
        {
            let mut enc = GzEncoder::new(
                std::io::BufWriter::new(File::create(&work).unwrap()),
                Compression::fast(),
            );
            for i in 0..pages {
                let ((giu, _), title) = page(i);
                // One or two files per page, shared between pages.
                for f in 0..1 + i % 2 {
                    let file = (i + f * 7919) % (pages * 2 / 3);
                    writeln!(enc, "{giu}\t0\t{title}\tFile_{file}.jpg").unwrap();
                }
            }
            enc.finish().unwrap();
        }
        eprintln!("bench: input written in {:.1?}", t.elapsed());

        let mut codes = DumpCodes {
            baglama: None,
            known: wikis
                .iter()
                .map(|(giu, code)| {
                    (
                        Dbname::parse(giu).unwrap(),
                        Some(DumpCode::parse(code).unwrap()),
                    )
                })
                .collect(),
        };
        let t = Instant::now();
        let (mut views, needed) =
            collect_page_keys(std::slice::from_ref(&work), &mut codes).unwrap();
        eprintln!(
            "bench: page keys in {:.1?}: {} pages on {} wikis",
            t.elapsed(),
            views.len(),
            needed.len()
        );
        for i in (0..pages).step_by(3) {
            let ((_, code), title) = page(i);
            views.add(
                page_key(code.as_bytes(), title.as_bytes()),
                (i % 1000) as u64,
            );
        }

        let t = Instant::now();
        let ym = YearMonth::new(2026, 9).unwrap();
        let out = gzb_path(&dir, GroupId::new(979).unwrap(), &ym);
        let header = build_group_file(
            GroupId::new(979).unwrap(),
            &ym,
            &work,
            &out,
            &views,
            &mut codes,
        )
        .unwrap();
        eprintln!(
            "bench: file built in {:.1?}: {} pages, {} views, {} bytes",
            t.elapsed(),
            header.total_pages,
            header.total_views,
            std::fs::metadata(&out).unwrap().len()
        );
        assert_eq!(header.total_pages as usize, pages);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Reference output of [`test_golden_file`], written by the code of
    /// 2026-10-09. The PHP reader in glamtools reads it the same way.
    const GOLDEN: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/gzb/testdata/golden_42.gzb"
    );

    /// Blanks the `"created":"…"` timestamp, the only part allowed to differ.
    fn without_created(bytes: &[u8]) -> Vec<u8> {
        let text = String::from_utf8_lossy(bytes);
        let start = text.find("\"created\":\"").expect("created") + 11;
        let end = start + text[start..].find('"').expect("end of created");
        let mut ret = bytes.to_vec();
        ret[start..end].fill(b'X');
        ret
    }

    /// Phase 3 on fixed inputs must give exactly [`GOLDEN`], so a change to
    /// what gets written cannot slip through. After an intended format change,
    /// regenerate it: `UPDATE_GOLDEN=1 cargo test test_golden_file`.
    #[test]
    fn test_golden_file() {
        let dir = std::env::temp_dir().join(format!("gzb_golden_{}", std::process::id()));
        let out = dir.join("42.gzb");
        std::fs::create_dir_all(&dir).unwrap();
        let work = dir.join("42.tsv.gz");
        let wikis = [
            ("enwiki", "en.wikipedia"),
            ("dewiki", "de.wikipedia"),
            ("zh_min_nanwiki", "zh-min-nan.wikipedia"),
            ("wikidatawiki", "wikidata"),
            ("commonswiki", "commons.wikimedia"),
        ];
        let mut lines = vec![];
        for i in 0..5000usize {
            let (giu, _) = wikis[i % wikis.len()];
            let ns = [0, 0, 14, 4, 0][i % 5];
            let title = format!("Ünïcødé_{}_{}", i % 1700, ["Zürich", "東京", "x"][i % 3]);
            lines.push(format!("{giu}\t{ns}\t{title}\tFile_{}.jpg", (i * 7) % 900));
        }
        lines.push("xxwiki\t0\tNowhere\tA.jpg".to_string());
        lines.push("not a usage line".to_string());
        {
            let mut enc = GzEncoder::new(File::create(&work).unwrap(), Compression::fast());
            for line in &lines {
                writeln!(enc, "{line}").unwrap();
            }
            enc.finish().unwrap();
        }
        let mut codes = DumpCodes {
            baglama: None,
            known: wikis
                .iter()
                .map(|(g, c)| (Dbname::parse(g).unwrap(), Some(DumpCode::parse(c).unwrap())))
                .collect(),
        };
        let (mut views, _) = collect_page_keys(std::slice::from_ref(&work), &mut codes).unwrap();
        for (i, line) in lines.iter().enumerate().step_by(3) {
            if let Some((giu, _, title, _)) = parse_work_line(line) {
                if let Some(code) = codes.get(giu) {
                    views.add(
                        page_key(code.as_bytes(), title.as_bytes()),
                        (i * 37 % 5000) as u64,
                    );
                }
            }
        }
        let ym = YearMonth::new(2026, 9).unwrap();
        build_group_file(
            GroupId::new(42).unwrap(),
            &ym,
            &work,
            &out,
            &views,
            &mut codes,
        )
        .unwrap();
        let written = std::fs::read(&out).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            std::fs::write(GOLDEN, &written).unwrap();
        }
        let golden = std::fs::read(GOLDEN).unwrap();
        assert!(
            without_created(&written) == without_created(&golden),
            "gzb output differs from {GOLDEN}"
        );
    }

    #[test]
    fn test_build_group_file() {
        let dir = std::env::temp_dir().join(format!("gzb_build_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let work = dir.join("5.tsv.gz");
        {
            let mut enc = GzEncoder::new(File::create(&work).unwrap(), Compression::fast());
            for line in [
                "enwiki\t0\tRome\tB.jpg",
                "dewiki\t14\tKategorie:Rom\tA.jpg",
                "enwiki\t0\tItaly\tA.jpg",
                "enwiki\t0\tRome\tA.jpg",
                "enwiki\t0\tRome\tB.jpg", // same usage twice
                "enwiki\t0\tBern\tC.jpg",
                "enwiki\t0\tAachen\tC.jpg",
                "xxwiki\t0\tNowhere\tA.jpg",
                "not a usage line",
            ] {
                writeln!(enc, "{line}").unwrap();
            }
            enc.finish().unwrap();
        }
        let mut views = ViewTable::from_keys(
            [&b"Rome"[..], b"Italy", b"Bern", b"Aachen"]
                .iter()
                .map(|t| page_key(b"en.wikipedia", t))
                .chain([page_key(b"de.wikipedia", b"Kategorie:Rom")])
                .collect(),
        );
        views.add(page_key(b"en.wikipedia", b"Rome"), 70);
        views.add(page_key(b"en.wikipedia", b"Italy"), 500);
        views.add(page_key(b"de.wikipedia", b"Kategorie:Rom"), 3);
        let mut codes = DumpCodes {
            baglama: None,
            known: [("enwiki", "en.wikipedia"), ("dewiki", "de.wikipedia")]
                .into_iter()
                .map(|(g, c)| (Dbname::parse(g).unwrap(), Some(DumpCode::parse(c).unwrap())))
                .collect(),
        };
        let ym = YearMonth::new(2026, 9).unwrap();
        let out = gzb_path(&dir, GroupId::new(5).unwrap(), &ym);
        let header = build_group_file(
            GroupId::new(5).unwrap(),
            &ym,
            &work,
            &out,
            &views,
            &mut codes,
        )
        .unwrap();

        assert_eq!(header.total_views, 573);
        assert_eq!(header.total_pages, 6);
        let giu: Vec<&str> = header.sites.iter().map(|s| s.giu.as_str()).collect();
        assert_eq!(giu, vec!["enwiki", "dewiki", "xxwiki"]);
        let mut r = GzbReader::open(&out).unwrap();
        let en = r.rows("enwiki", 0).unwrap();
        let summary: Vec<(&str, u64)> = en.iter().map(|r| (r.title.as_str(), r.views)).collect();
        // Views descending, then title ascending.
        assert_eq!(
            summary,
            vec![("Italy", 500), ("Rome", 70), ("Aachen", 0), ("Bern", 0)]
        );
        assert_eq!(en[1].files, vec!["A.jpg", "B.jpg"]);
        let de = r.rows("dewiki", 0).unwrap();
        assert_eq!((de[0].namespace_id, de[0].views), (14, 3));
        assert_eq!(r.rows("xxwiki", 0).unwrap()[0].views, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
