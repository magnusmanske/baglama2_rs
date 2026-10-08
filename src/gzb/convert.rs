//! `gzb_convert`: rewrite completed legacy group-months as gzb files.
//!
//! Sources, by `group_status.storage`:
//! - `file` (2010–2014): tab-separated flat file named in `group_status.file`.
//! - `mysql` (2014–2018): `group2view`/`views`/`pages`/`files`/`gs2site` in
//!   the tool DB.
//! - `sqlite3` (2018–2024): one SQLite file per group-month.
//!
//! `mysql2` rows are not converted: their page lists never got view counts.
//! Regenerate those months with `gzb_month`.
//!
//! Sources are never modified or deleted. A converted row only has its
//! `storage` switched to `gzb`; every switch is appended to
//! `<gzb root>/conversion.log` (`time, group_status id, group, YYYYMM, old
//! storage, source`), which is all it takes to switch back.

use super::*;
use crate::db_mysql2::DbMySql2;
use crate::Baglama2;
use log::{error, info, warn};
use mysql_async::prelude::*;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub const CONVERTIBLE: &[&str] = &["file", "mysql", "sqlite3"];

/// Source file size that counts as one unit of conversion concurrency.
const SOURCE_WEIGHT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub storages: Vec<String>,
    /// Inclusive, as `YYYYMM`.
    pub from: Option<u32>,
    pub to: Option<u32>,
    pub group_ids: Option<Vec<usize>>,
    pub limit: Option<usize>,
    pub jobs: usize,
    pub dry_run: bool,
    /// Write and verify files, but leave `group_status` as it is.
    pub no_switch: bool,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            storages: CONVERTIBLE.iter().map(|s| s.to_string()).collect(),
            from: None,
            to: None,
            group_ids: None,
            limit: None,
            jobs: 3,
            dry_run: false,
            no_switch: false,
        }
    }
}

#[derive(Debug, Clone)]
struct Job {
    gs_id: usize,
    group_id: usize,
    ym: YearMonth,
    storage: String,
    file: String,
    sqlite3: String,
}

pub async fn run(baglama: Arc<Baglama2>, opts: ConvertOptions) -> Result<()> {
    for s in &opts.storages {
        if !CONVERTIBLE.contains(&s.as_str()) {
            return Err(anyhow!(
                "cannot convert storage '{s}'; choose from {CONVERTIBLE:?}"
            ));
        }
    }
    if !opts.dry_run && !opts.no_switch {
        ensure_storage_enum(&baglama).await?;
    }
    report_mysql2(&baglama).await?;

    let jobs = select_jobs(&baglama, &opts).await?;
    info!("gzb_convert: {} group-months to convert", jobs.len());
    let root = baglama.gzb_data_root_path();
    let sqlite_root = PathBuf::from(baglama.sqlite_data_root_path());

    if opts.dry_run {
        return dry_run(&jobs, &sqlite_root);
    }

    let n_jobs = opts.jobs.max(1);
    let semaphore = Arc::new(Semaphore::new(n_jobs));
    let mut join_set = tokio::task::JoinSet::new();
    let total = jobs.len();
    for (n, job) in jobs.into_iter().enumerate() {
        let weight = source_file(&sqlite_root, &job)
            .and_then(|p| std::fs::metadata(p).ok())
            .map_or(1, |m| {
                (m.len() / SOURCE_WEIGHT_BYTES).clamp(1, n_jobs as u64)
            }) as u32;
        let permit = semaphore
            .clone()
            .acquire_many_owned(weight)
            .await
            .expect("semaphore closed");
        let baglama = baglama.clone();
        let root = root.clone();
        let sqlite_root = sqlite_root.clone();
        let no_switch = opts.no_switch;
        join_set.spawn(async move {
            let label = format!(
                "[{}/{total}] group {} {} ({})",
                n + 1,
                job.group_id,
                job.ym,
                job.storage
            );
            let res = convert_one(&baglama, &root, &sqlite_root, &job, no_switch).await;
            drop(permit);
            match res {
                Ok(header) => {
                    info!(
                        "{label}: {} pages on {} wikis, {} views",
                        header.total_pages,
                        header.sites.len(),
                        header.total_views
                    );
                    true
                }
                Err(e) => {
                    error!("{label}: {e:#}");
                    false
                }
            }
        });
    }
    let (mut ok, mut failed) = (0, 0);
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(true) => ok += 1,
            _ => failed += 1,
        }
    }
    info!("gzb_convert: {ok} converted, {failed} failed");
    if failed > 0 {
        return Err(anyhow!(
            "{failed} conversions failed; their rows were left unchanged"
        ));
    }
    Ok(())
}

async fn report_mysql2(baglama: &Baglama2) -> Result<()> {
    let rows: Vec<(i32, u32, u64)> = baglama
        .get_tooldb_conn()
        .await?
        .query(
            "SELECT year,month,COUNT(*) FROM group_status WHERE storage='mysql2' GROUP BY year,month",
        )
        .await?;
    for (year, month, n) in rows {
        warn!(
            "{n} mysql2 rows for {year}-{month:02} have no view counts and are not converted; \
             regenerate with: baglama2 gzb_month {year} {month}"
        );
    }
    Ok(())
}

async fn select_jobs(baglama: &Baglama2, opts: &ConvertOptions) -> Result<Vec<Job>> {
    let mut sql = format!(
        "SELECT id,group_id,year,month,storage,IFNULL(file,''),IFNULL(sqlite3,'') FROM group_status
         WHERE status='VIEW DATA COMPLETE' AND storage IN ({})",
        Baglama2::sql_placeholders(opts.storages.len())
    );
    let mut params: Vec<mysql_async::Value> = opts.storages.iter().map(|s| s.into()).collect();
    if let Some(from) = opts.from {
        sql += " AND year*100+month>=?";
        params.push(from.into());
    }
    if let Some(to) = opts.to {
        sql += " AND year*100+month<=?";
        params.push(to.into());
    }
    if let Some(ids) = &opts.group_ids {
        sql += &format!(
            " AND group_id IN ({})",
            Baglama2::sql_placeholders(ids.len())
        );
        params.extend(ids.iter().map(|id| (*id as u64).into()));
    }
    sql += " ORDER BY year,month,group_id";
    if let Some(limit) = opts.limit {
        sql += &format!(" LIMIT {limit}");
    }
    let rows: Vec<(usize, usize, i32, u32, String, String, String)> =
        baglama.get_tooldb_conn().await?.exec(sql, params).await?;
    rows.into_iter()
        .map(|(gs_id, group_id, year, month, storage, file, sqlite3)| {
            Ok(Job {
                gs_id,
                group_id,
                ym: YearMonth::new(year, month)?,
                storage,
                file,
                sqlite3,
            })
        })
        .collect()
}

fn dry_run(jobs: &[Job], sqlite_root: &Path) -> Result<()> {
    let mut totals: HashMap<String, (u64, u64, u64)> = HashMap::new(); // n, missing, bytes
    for job in jobs {
        let source = source_file(sqlite_root, job);
        let entry = totals.entry(job.storage.clone()).or_default();
        entry.0 += 1;
        if let Some(path) = source {
            match std::fs::metadata(&path) {
                Ok(m) => entry.2 += m.len(),
                Err(_) => {
                    entry.1 += 1;
                    println!(
                        "missing: group {} {} {}",
                        job.group_id,
                        job.ym,
                        path.display()
                    );
                }
            }
        }
    }
    for (storage, (n, missing, bytes)) in totals {
        println!(
            "{storage}: {n} group-months, {missing} sources missing, {:.1} GB of source files",
            bytes as f64 / 1e9
        );
    }
    Ok(())
}

async fn convert_one(
    baglama: &Baglama2,
    root: &Path,
    sqlite_root: &Path,
    job: &Job,
    no_switch: bool,
) -> Result<GzbHeader> {
    let out = gzb_path(root, job.group_id, &job.ym);
    let (header, source) = match job.storage.as_str() {
        "sqlite3" => {
            let path = sqlite_source(sqlite_root, job)
                .ok_or_else(|| anyhow!("no SQLite file for group {} {}", job.group_id, job.ym))?;
            let (j, o, p) = (job.clone(), out.clone(), path.clone());
            let header =
                tokio::task::spawn_blocking(move || convert_sqlite(&p, j.group_id, &j.ym, &o))
                    .await??;
            (header, path.display().to_string())
        }
        "file" => {
            let path = PathBuf::from(&job.file);
            let (j, o, p) = (job.clone(), out.clone(), path.clone());
            let header =
                tokio::task::spawn_blocking(move || convert_flat_file(&p, j.group_id, &j.ym, &o))
                    .await??;
            (header, path.display().to_string())
        }
        "mysql" => (
            convert_mysql(baglama, job, &out).await?,
            format!("group_status_id={}", job.gs_id),
        ),
        other => return Err(anyhow!("cannot convert storage '{other}'")),
    };

    // Read back before pointing the API at it.
    let back = GzbReader::open(&out)?;
    if back.header() != &header {
        return Err(anyhow!("{} does not read back as written", out.display()));
    }
    if no_switch {
        return Ok(header);
    }

    let mut conn = baglama.get_tooldb_conn().await?;
    conn.exec_drop(
        "UPDATE group_status SET storage=? WHERE id=? AND storage=?",
        (STORAGE, job.gs_id, &job.storage),
    )
    .await?;
    if conn.affected_rows() != 1 {
        return Err(anyhow!(
            "group_status {} changed underneath; left as is",
            job.gs_id
        ));
    }
    append_conversion_log(root, job, &source)?;
    Ok(header)
}

fn append_conversion_log(root: &Path, job: &Job, source: &str) -> Result<()> {
    use std::io::Write;
    let line = format!(
        "{}\t{}\t{}\t{}\t{}\t{}\n",
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        job.gs_id,
        job.group_id,
        year_month_dir(&job.ym),
        job.storage,
        source
    );
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("conversion.log"))?
        .write_all(line.as_bytes())?;
    Ok(())
}

/// The legacy file a job reads, if it reads one.
fn source_file(sqlite_root: &Path, job: &Job) -> Option<PathBuf> {
    match job.storage.as_str() {
        "sqlite3" => sqlite_source(sqlite_root, job),
        "file" => Some(PathBuf::from(&job.file)),
        _ => None,
    }
}

/// Where the legacy SQLite file of a group-month is: the stored path for old
/// rows, else `<root>/<YYYYMM>/<gid>.sqlite3`, or `.sqlite` as written by
/// some Rust builds — the same rule as the PHP API.
fn sqlite_source(sqlite_root: &Path, job: &Job) -> Option<PathBuf> {
    if !job.sqlite3.is_empty() {
        return Some(PathBuf::from(&job.sqlite3));
    }
    let dir = sqlite_root.join(year_month_dir(&job.ym));
    [
        dir.join(format!("{}.sqlite3", job.group_id)),
        dir.join(format!("{}.sqlite", job.group_id)),
    ]
    .into_iter()
    .find(|p| p.is_file())
}

/// Legacy rows, compactly, as in `gzb_month`: titles are slices of one
/// shared buffer and file names are interned, so a page costs ~40 bytes plus
/// its title. Owned `GzbRow`s in hash maps took several hundred, which ran
/// conversions of the biggest group-months out of memory.
#[derive(Default)]
struct CompactRows {
    titles: Vec<u8>,
    rows: Vec<CompactRow>,
    /// `(row, file)`: indexes into `rows` and the interned file names.
    usages: Vec<(u32, u32)>,
    file_ids: HashMap<String, u32>,
    /// Legacy site ID → index into `sites`.
    site_ids: HashMap<String, u32>,
    sites: Vec<LegacySite>,
}

struct CompactRow {
    views: u64,
    title_start: u32,
    title_len: u32,
    namespace_id: i32,
    site: u32,
}

#[derive(Default)]
struct LegacySite {
    giu: Option<String>,
    /// Stored `(pages, views)`, which override the computed ones.
    summary: Option<(u64, u64)>,
}

impl CompactRows {
    /// Index of a legacy site, by its ID in the source.
    fn site(&mut self, id: &str) -> Result<u32> {
        let n = intern(&mut self.site_ids, id)?;
        if n as usize == self.sites.len() {
            self.sites.push(LegacySite::default());
        }
        Ok(n)
    }

    fn site_mut(&mut self, id: &str) -> Result<&mut LegacySite> {
        let n = self.site(id)?;
        Ok(&mut self.sites[n as usize])
    }

    /// Adds a row, returning its index for [`CompactRows::add_file`].
    fn push(&mut self, site: u32, title: &str, namespace_id: i32, views: u64) -> Result<u32> {
        let row = u32::try_from(self.rows.len()).map_err(|_| anyhow!("too many rows"))?;
        let title_start =
            u32::try_from(self.titles.len()).map_err(|_| anyhow!("over 4 GB of titles"))?;
        self.titles.extend_from_slice(title.as_bytes());
        self.rows.push(CompactRow {
            views,
            title_start,
            title_len: title.len() as u32,
            namespace_id,
            site,
        });
        Ok(row)
    }

    fn add_file(&mut self, row: u32, name: &str) -> Result<()> {
        if !name.is_empty() {
            let file = intern(&mut self.file_ids, name)?;
            self.usages.push((row, file));
        }
        Ok(())
    }

    /// Writes every site that has rows or a stored summary. Rows of sites
    /// without a `giu` are dropped with a warning naming `source`.
    fn write(self, writer: &mut GzbWriter, source: &str) -> Result<()> {
        let Self {
            titles,
            rows,
            mut usages,
            file_ids,
            site_ids,
            sites,
        } = self;
        let files = interned_list(file_ids);
        let site_names = interned_list(site_ids);
        usages.sort_unstable();
        usages.dedup();
        // Titles were &str when stored, so this cannot fail.
        let title = |r: &CompactRow| {
            std::str::from_utf8(&titles[r.title_start as usize..][..r.title_len as usize])
                .unwrap_or_default()
        };

        let mut order: Vec<u32> = (0..rows.len() as u32).collect();
        order.sort_unstable_by(|&a, &b| {
            let (ra, rb) = (&rows[a as usize], &rows[b as usize]);
            ra.site
                .cmp(&rb.site)
                .then(rb.views.cmp(&ra.views))
                .then_with(|| title(ra).cmp(title(rb)))
                .then(ra.namespace_id.cmp(&rb.namespace_id))
                .then(a.cmp(&b))
        });

        // Each site's slice of `order`, empty for summary-only sites.
        let mut ranges = vec![0..0; sites.len()];
        let mut start = 0;
        while start < order.len() {
            let site = rows[order[start] as usize].site;
            let end = order[start..]
                .iter()
                .position(|&r| rows[r as usize].site != site)
                .map_or(order.len(), |p| start + p);
            ranges[site as usize] = start..end;
            start = end;
        }

        let mut names: Vec<&str> = vec![];
        for (n, (site, range)) in sites.iter().zip(ranges).enumerate() {
            if range.is_empty() && site.summary.is_none() {
                continue;
            }
            let Some(giu) = &site.giu else {
                warn!(
                    "{source}: site id {} unknown, dropping {} rows",
                    site_names[n],
                    range.len()
                );
                continue;
            };
            let site_rows = &order[range];
            let (pages, views) = site.summary.unwrap_or_else(|| {
                (
                    site_rows.len() as u64,
                    site_rows.iter().map(|&r| rows[r as usize].views).sum(),
                )
            });
            writer.add_site_sorted(giu, site_rows.len(), pages, views, |i, buf| {
                let r = site_rows[i];
                let row = &rows[r as usize];
                let first = usages.partition_point(|u| u.0 < r);
                names.clear();
                names.extend(
                    usages[first..]
                        .iter()
                        .take_while(|u| u.0 == r)
                        .map(|u| files[u.1 as usize].as_str()),
                );
                names.sort_unstable();
                write_row(
                    buf,
                    title(row),
                    row.namespace_id,
                    row.views,
                    names.iter().copied(),
                );
            })?;
        }
        Ok(())
    }
}

fn convert_sqlite(path: &Path, group_id: usize, ym: &YearMonth, out: &Path) -> Result<GzbHeader> {
    use rusqlite::{Connection, OpenFlags};
    if std::fs::metadata(path)?.len() == 0 {
        return Err(anyhow!("{} is empty", path.display()));
    }
    // immutable=1: read-only, and no locking, which NFS does badly.
    let conn = Connection::open_with_flags(
        format!("file:{}?immutable=1", path.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let mut data = CompactRows::default();

    let mut stmt = conn.prepare("SELECT id,giu_code FROM sites WHERE giu_code IS NOT NULL")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        data.site_mut(&r.get::<_, i64>(0)?.to_string())?.giu = Some(r.get(1)?);
    }

    // Per-site totals as the API has been showing them; first row wins, as
    // duplicates are not expected.
    let mut stmt = conn.prepare("SELECT site_id,pages,views FROM gs2site")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let (pages, views) = (r.get::<_, i64>(1)?, r.get::<_, i64>(2)?);
        let site = data.site_mut(&r.get::<_, i64>(0)?.to_string())?;
        site.summary
            .get_or_insert((pages.max(0) as u64, views.max(0) as u64));
    }

    // Only `done=1` rows, which is what the API lists. Two plain scans
    // instead of a join: `group2view.view_id` has no index, so a join makes
    // SQLite build one in memory.
    let mut view_rows: Vec<(i64, u32)> = vec![];
    let mut site_cache: Option<(i64, u32)> = None;
    let mut stmt =
        conn.prepare("SELECT id,site,title,namespace_id,views FROM views WHERE done=1")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let site_id: i64 = r.get(1)?;
        let site = match site_cache {
            Some((id, n)) if id == site_id => n,
            _ => {
                let n = data.site(&site_id.to_string())?;
                site_cache = Some((site_id, n));
                n
            }
        };
        let row = data.push(
            site,
            r.get_ref(2)?.as_str()?,
            r.get(3)?,
            r.get::<_, i64>(4)?.max(0) as u64,
        )?;
        view_rows.push((r.get(0)?, row));
    }
    view_rows.sort_unstable();

    let mut stmt = conn.prepare("SELECT view_id,image FROM group2view")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let view_id: i64 = r.get(0)?;
        let Ok(i) = view_rows.binary_search_by_key(&view_id, |v| v.0) else {
            continue;
        };
        if let Some(image) = r.get_ref(1)?.as_str_or_null()? {
            data.add_file(view_rows[i].1, image)?;
        }
    }
    drop(view_rows);

    let mut writer = GzbWriter::new(out, group_id, ym, "sqlite3");
    data.write(&mut writer, &path.display().to_string())?;
    writer.finish()
}

fn convert_flat_file(
    path: &Path,
    group_id: usize,
    ym: &YearMonth,
    out: &Path,
) -> Result<GzbHeader> {
    let mut data = CompactRows::default();
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = vec![];
    // Header row first; then `giu \t urlencoded title \t files \t views`.
    reader.read_until(b'\n', &mut buf)?;
    loop {
        buf.clear();
        if reader.read_until(b'\n', &mut buf)? == 0 {
            break;
        }
        let line = String::from_utf8_lossy(&buf);
        let line = line.trim_end_matches('\n').trim_end_matches('\r');
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 4 {
            continue;
        }
        let site = data.site(cols[0])?;
        data.sites[site as usize]
            .giu
            .get_or_insert_with(|| cols[0].to_string());
        let row = data.push(
            site,
            &url_decode(cols[1]).replace(' ', "_"),
            0,
            cols[3].trim().parse().unwrap_or(0),
        )?;
        for file in cols[2].split('|') {
            data.add_file(row, file)?;
        }
    }
    let mut writer = GzbWriter::new(out, group_id, ym, "file");
    data.write(&mut writer, &path.display().to_string())?;
    writer.finish()
}

async fn convert_mysql(baglama: &Baglama2, job: &Job, out: &Path) -> Result<GzbHeader> {
    let mut data = CompactRows::default();
    for site in baglama.get_sites()? {
        if let Some(giu) = site.giu_code() {
            data.site_mut(&site.id().to_string())?.giu = Some(giu.clone());
        }
    }
    let mut conn = baglama.get_tooldb_conn().await?;

    // First row wins.
    let summaries: Vec<(usize, u64, u64)> = conn
        .exec(
            "SELECT site_id,pages,views FROM gs2site WHERE group_status_id=?",
            (job.gs_id,),
        )
        .await?;
    for (site, pages, views) in summaries {
        data.site_mut(&site.to_string())?
            .summary
            .get_or_insert((pages, views));
    }

    let mut view_rows: HashMap<usize, u32> = HashMap::new();
    let mut failed: Option<anyhow::Error> = None;
    conn.exec_iter(
        "SELECT g.view_id,v.site,FROM_BASE64(TO_BASE64(p.title)),p.namespace_id,v.views,
                FROM_BASE64(TO_BASE64(f.name))
         FROM group2view g
         JOIN views v ON v.id=g.view_id
         LEFT JOIN pages p ON p.id=v.pages_id
         LEFT JOIN files f ON f.id=g.file_id
         WHERE g.group_status_id=?",
        (job.gs_id,),
    )
    .await?
    .for_each_and_drop(|row: mysql_async::Row| {
        if failed.is_some() {
            return;
        }
        let res = (|| -> Result<()> {
            let Some(Some(view_id)) = row.get::<Option<usize>, _>(0) else {
                return Ok(());
            };
            let Some(Some(site)) = row.get::<Option<usize>, _>(1) else {
                return Ok(());
            };
            let r = match view_rows.get(&view_id) {
                Some(r) => *r,
                None => {
                    let title = row
                        .get::<Option<Vec<u8>>, _>(2)
                        .flatten()
                        .map(|b| repair_title(&String::from_utf8_lossy(&b)))
                        .unwrap_or_default();
                    let site = data.site(&site.to_string())?;
                    let r = data.push(
                        site,
                        &title,
                        row.get::<Option<i32>, _>(3).flatten().unwrap_or(0),
                        row.get::<Option<i64>, _>(4).flatten().unwrap_or(0).max(0) as u64,
                    )?;
                    view_rows.insert(view_id, r);
                    r
                }
            };
            if let Some(Some(name)) = row.get::<Option<Vec<u8>>, _>(5) {
                data.add_file(r, &String::from_utf8_lossy(&name))?;
            }
            Ok(())
        })();
        if let Err(e) = res {
            failed = Some(e);
        }
    })
    .await?;
    if let Some(e) = failed {
        return Err(e);
    }
    drop(view_rows);

    let source = format!("group_status {}", job.gs_id);
    let mut writer = GzbWriter::new(out, job.group_id, &job.ym, "mysql");
    tokio::task::spawn_blocking(move || {
        data.write(&mut writer, &source)?;
        writer.finish()
    })
    .await?
}

/// Undo the double UTF-8 encoding found in the tool DB's `pages.title`.
fn repair_title(s: &str) -> String {
    match DbMySql2::repair_double_encoding(s) {
        Some(fixed) => fixed,
        None => s.to_string(),
    }
}

/// PHP `urldecode`: `%XX` escapes and `+` as space.
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_decode() {
        assert_eq!(url_decode("K%C3%B6ln"), "Köln");
        assert_eq!(url_decode("A+B"), "A B");
        assert_eq!(url_decode("C%2B%2B"), "C++");
        assert_eq!(url_decode("100%"), "100%");
        assert_eq!(url_decode("50%zz"), "50%zz");
        assert_eq!(url_decode("x%4"), "x%4");
    }

    #[test]
    fn test_repair_title() {
        assert_eq!(repair_title("AlcalÃ¡"), "Alcalá");
        assert_eq!(repair_title("Alcalá"), "Alcalá");
        assert_eq!(repair_title("Plain"), "Plain");
    }

    #[test]
    fn test_convert_flat_file() {
        let dir = std::env::temp_dir().join(format!("gzb_flat_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("x.tab");
        std::fs::write(
            &src,
            "site\ttitle\tfiles\tviews\nenwiki\tK%C3%B6ln_Cathedral\tA.jpg|B.jpg\t10\n\
             enwiki\tBonn\tC.jpg\t30\ndewiki\tK%C3%B6ln\tA.jpg\t5\n\n",
        )
        .unwrap();
        let ym = YearMonth::new(2012, 5).unwrap();
        let out = gzb_path(&dir, 7, &ym);
        let header = convert_flat_file(&src, 7, &ym, &out).unwrap();
        assert_eq!(header.total_views, 45);
        assert_eq!(header.sites[0].giu, "enwiki");
        assert_eq!(header.sites[0].pages, 2);
        let mut r = GzbReader::open(&out).unwrap();
        let en = r.rows("enwiki", 0).unwrap();
        assert_eq!(en[0].title, "Bonn");
        assert_eq!(en[1].title, "Köln_Cathedral");
        assert_eq!(en[1].files, vec!["A.jpg", "B.jpg"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_convert_sqlite() {
        let dir = std::env::temp_dir().join(format!("gzb_sqlite_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("9.sqlite3");
        {
            let c = rusqlite::Connection::open(&src).unwrap();
            c.execute_batch(&std::fs::read_to_string("baglama.sqlite3_schema").unwrap())
                .unwrap();
            c.execute_batch(
                "INSERT INTO sites VALUES (1,'en','en.wikipedia.org','enwiki','wikipedia','en','');
                 INSERT INTO sites VALUES (2,'de','de.wikipedia.org','dewiki','wikipedia','de','');
                 INSERT INTO views VALUES (1,1,'Italy',1,2022,1,0,10,500);
                 INSERT INTO views VALUES (2,1,'Rome',1,2022,1,0,11,700);
                 INSERT INTO views VALUES (3,1,'Broken',1,2022,2,0,12,0);
                 INSERT INTO views VALUES (4,2,'Rom',1,2022,1,0,13,70);
                 INSERT INTO group2view VALUES (1,5,1,'A.jpg');
                 INSERT INTO group2view VALUES (2,5,1,'B.jpg');
                 INSERT INTO group2view VALUES (3,5,2,'A.jpg');
                 INSERT INTO gs2site VALUES (1,5,1,3,1200);",
            )
            .unwrap();
        }
        let ym = YearMonth::new(2022, 1).unwrap();
        let out = gzb_path(&dir, 9, &ym);
        let header = convert_sqlite(&src, 9, &ym, &out).unwrap();
        // gs2site wins for enwiki (3 pages incl. the not-done one); dewiki computed.
        assert_eq!(header.site("enwiki").unwrap().pages, 3);
        assert_eq!(header.site("enwiki").unwrap().views, 1200);
        assert_eq!(header.site("dewiki").unwrap().views, 70);
        let mut r = GzbReader::open(&out).unwrap();
        let en = r.rows("enwiki", 0).unwrap();
        assert_eq!(en.len(), 2);
        assert_eq!(en[0].title, "Rome");
        assert_eq!(en[1].files, vec!["A.jpg", "B.jpg"]);
        assert!(r.rows("dewiki", 0).unwrap()[0].files.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
