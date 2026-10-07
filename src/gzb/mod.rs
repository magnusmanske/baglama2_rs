//! "gzb" view-data storage: one compressed file per (group, month).
//!
//! Replaces the per-month `viewdata_*` MySQL tables and the per-group SQLite
//! files. The web API only ever asks two things of a group-month — per-site
//! totals, and one site's pages sorted by views — so the file is laid out to
//! answer exactly those without a database:
//!
//! ```text
//! BAGLAMA-GZB <version> <header_len>\n      ASCII, one line
//! <header_len bytes>                         JSON, see GzbHeader
//! <data>                                     independent gzip members
//! ```
//!
//! Every site's rows are sorted by views (descending) and split into chunks
//! of [`CHUNK_ROWS`] rows; each chunk is its own gzip member. The header lists
//! per-site totals and each chunk's `offset` (relative to the start of the
//! data area) and `length`. A reader wanting the top 100 pages of one site
//! seeks to that site's first chunk and inflates only that. Plain gzip keeps
//! it readable from PHP (`gzdecode`) without extensions.
//!
//! Chunk rows are TSV: `title \t namespace_id \t views \t file1|file2|…`.
//! Titles are underscored. Freshly generated data carries the local
//! namespace prefix in the title (as in the pageview dump); converted legacy
//! data keeps whatever form the legacy store had.

pub mod convert;
pub mod month;
pub mod tsv;

use crate::YearMonth;
use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const MAGIC: &str = "BAGLAMA-GZB";
pub const VERSION: u32 = 1;
pub const FILE_EXTENSION: &str = "gzb";

/// Value of `group_status.storage` for group-months stored in this format.
pub const STORAGE: &str = "gzb";

/// Rows per gzip member. Small enough that the API's usual "top 100" request
/// inflates a few hundred KB, large enough to compress well.
pub const CHUNK_ROWS: usize = 5000;

/// `group_status.total_views` is a signed INT(11).
pub const MAX_DB_VIEWS: u64 = i32::MAX as u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GzbChunk {
    pub offset: u64,
    pub length: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GzbSiteHeader {
    /// Wiki database name, e.g. `enwiki` (`sites.giu_code`).
    pub giu: String,
    /// Distinct pages on this wiki, as shown in the month overview.
    pub pages: u64,
    /// Sum of views on this wiki, as shown in the month overview.
    pub views: u64,
    pub chunks: Vec<GzbChunk>,
}

impl GzbSiteHeader {
    pub fn rows(&self) -> u64 {
        self.chunks.iter().map(|c| c.rows).sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GzbHeader {
    pub version: u32,
    pub group_id: usize,
    pub year: i32,
    pub month: u32,
    /// Where the data came from: `dump` (generated), or the legacy storage
    /// it was converted from (`sqlite3`, `mysql`, `file`).
    pub source: String,
    pub created: String,
    pub total_views: u64,
    pub total_pages: u64,
    /// Sorted by views, descending.
    pub sites: Vec<GzbSiteHeader>,
}

impl GzbHeader {
    pub fn site(&self, giu: &str) -> Option<&GzbSiteHeader> {
        self.sites.iter().find(|s| s.giu == giu)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GzbRow {
    pub title: String,
    pub namespace_id: i32,
    pub views: u64,
    pub files: Vec<String>,
}

impl GzbRow {
    fn write_tsv(&self, out: &mut Vec<u8>) {
        write_row(
            out,
            &self.title,
            self.namespace_id,
            self.views,
            self.files.iter().map(|f| f.as_str()),
        );
    }

    fn parse_tsv(line: &str) -> Result<Self> {
        let mut cols = line.splitn(4, '\t');
        let title = cols.next().ok_or_else(|| anyhow!("missing title"))?;
        let namespace_id = cols
            .next()
            .ok_or_else(|| anyhow!("missing namespace"))?
            .parse()?;
        let views = cols
            .next()
            .ok_or_else(|| anyhow!("missing views"))?
            .parse()?;
        let files = cols.next().unwrap_or_default();
        let files = if files.is_empty() {
            vec![]
        } else {
            files.split('|').map(|s| s.to_string()).collect()
        };
        Ok(Self {
            title: title.to_string(),
            namespace_id,
            views,
            files,
        })
    }
}

/// Append one TSV row: `title \t namespace_id \t views \t file1|file2|…`.
pub fn write_row<'a>(
    out: &mut Vec<u8>,
    title: &str,
    namespace_id: i32,
    views: u64,
    files: impl IntoIterator<Item = &'a str>,
) {
    out.extend_from_slice(sanitize_field(title).as_bytes());
    out.push(b'\t');
    out.extend_from_slice(namespace_id.to_string().as_bytes());
    out.push(b'\t');
    out.extend_from_slice(views.to_string().as_bytes());
    out.push(b'\t');
    for (i, file) in files.into_iter().enumerate() {
        if i > 0 {
            out.push(b'|');
        }
        out.extend_from_slice(sanitize_field(file).replace('|', "_").as_bytes());
    }
    out.push(b'\n');
}

/// Titles and file names cannot legally contain tabs or newlines; make sure a
/// stray one in legacy data cannot break the row format.
fn sanitize_field(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains(['\t', '\n', '\r']) {
        std::borrow::Cow::Owned(s.replace(['\t', '\n', '\r'], " "))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Accumulates sites for one group-month, then writes the file atomically.
pub struct GzbWriter {
    header: GzbHeader,
    data: Vec<u8>,
}

impl GzbWriter {
    pub fn new(group_id: usize, ym: &YearMonth, source: &str) -> Self {
        Self {
            header: GzbHeader {
                version: VERSION,
                group_id,
                year: ym.year(),
                month: ym.month(),
                source: source.to_string(),
                created: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                total_views: 0,
                total_pages: 0,
                sites: vec![],
            },
            data: vec![],
        }
    }

    /// Add one site. Rows are sorted here; files per row are de-duplicated.
    /// `summary` overrides the computed `(pages, views)` — used when converting
    /// legacy data, whose stored per-site totals are what users have seen.
    pub fn add_site(
        &mut self,
        giu: &str,
        mut rows: Vec<GzbRow>,
        summary: Option<(u64, u64)>,
    ) -> Result<()> {
        for row in rows.iter_mut() {
            row.files.sort();
            row.files.dedup();
        }
        rows.sort_by(|a, b| {
            b.views
                .cmp(&a.views)
                .then_with(|| a.title.cmp(&b.title))
                .then_with(|| a.namespace_id.cmp(&b.namespace_id))
        });
        let (pages, views) =
            summary.unwrap_or_else(|| (rows.len() as u64, rows.iter().map(|r| r.views).sum()));
        self.add_site_sorted(giu, rows.len(), pages, views, |i, buf| {
            rows[i].write_tsv(buf)
        })
    }

    /// Add one site whose `n_rows` rows the caller has already sorted (views
    /// descending). `write_row(i, buf)` appends row `i` to `buf`, normally via
    /// [`write_row`]. Rows go straight into gzip chunks, so a caller with a
    /// compact representation never has to build a `GzbRow` per page.
    pub fn add_site_sorted(
        &mut self,
        giu: &str,
        n_rows: usize,
        pages: u64,
        views: u64,
        mut write_row: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<()> {
        if self.header.sites.iter().any(|s| s.giu == giu) {
            return Err(anyhow!("site {giu} added twice"));
        }
        let mut chunks = vec![];
        let mut buf = Vec::new();
        for start in (0..n_rows).step_by(CHUNK_ROWS) {
            let end = (start + CHUNK_ROWS).min(n_rows);
            buf.clear();
            for i in start..end {
                write_row(i, &mut buf);
            }
            let mut enc = GzEncoder::new(Vec::new(), Compression::best());
            enc.write_all(&buf)?;
            let compressed = enc.finish()?;
            chunks.push(GzbChunk {
                offset: self.data.len() as u64,
                length: compressed.len() as u64,
                rows: (end - start) as u64,
            });
            self.data.extend_from_slice(&compressed);
        }

        self.header.total_pages += pages;
        self.header.total_views += views;
        self.header.sites.push(GzbSiteHeader {
            giu: giu.to_string(),
            pages,
            views,
            chunks,
        });
        Ok(())
    }

    /// Write to `path` via a temporary file + rename, so a reader never sees
    /// a half-written file.
    pub fn finish(mut self, path: &Path) -> Result<GzbHeader> {
        self.header
            .sites
            .sort_by(|a, b| b.views.cmp(&a.views).then_with(|| a.giu.cmp(&b.giu)));
        let header_json = serde_json::to_vec(&self.header)?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension(format!("{FILE_EXTENSION}.tmp"));
        {
            let mut f = std::io::BufWriter::new(File::create(&tmp)?);
            writeln!(f, "{MAGIC} {VERSION} {}", header_json.len())?;
            f.write_all(&header_json)?;
            f.write_all(&self.data)?;
            f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(self.header)
    }
}

pub struct GzbReader {
    file: File,
    header: GzbHeader,
    data_start: u64,
}

impl GzbReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let (header, data_start) = {
            let mut reader = BufReader::new(&mut file);
            let mut first = String::new();
            reader.read_line(&mut first)?;
            let mut parts = first.trim_end().split(' ');
            if parts.next() != Some(MAGIC) {
                return Err(anyhow!("{}: not a gzb file", path.display()));
            }
            let version: u32 = parts.next().unwrap_or_default().parse()?;
            if version != VERSION {
                return Err(anyhow!("{}: unsupported version {version}", path.display()));
            }
            let header_len: usize = parts.next().unwrap_or_default().parse()?;
            let mut header_json = vec![0; header_len];
            reader.read_exact(&mut header_json)?;
            let header: GzbHeader = serde_json::from_slice(&header_json)?;
            (header, (first.len() + header_len) as u64)
        };
        Ok(Self {
            file,
            header,
            data_start,
        })
    }

    pub fn header(&self) -> &GzbHeader {
        &self.header
    }

    /// The top `max` rows (all if 0) of one site, sorted by views.
    pub fn rows(&mut self, giu: &str, max: usize) -> Result<Vec<GzbRow>> {
        let mut ret = vec![];
        self.for_each_row(giu, max, |row| {
            ret.push(row);
            Ok(())
        })?;
        Ok(ret)
    }

    /// Calls `f` on the top `max` rows (all if 0) of one site, in order,
    /// holding one chunk in memory at a time. Returns the number of rows.
    pub fn for_each_row(
        &mut self,
        giu: &str,
        max: usize,
        mut f: impl FnMut(GzbRow) -> Result<()>,
    ) -> Result<usize> {
        let chunks = match self.header.site(giu) {
            Some(site) => site.chunks.clone(),
            None => return Ok(0),
        };
        let mut n = 0;
        for chunk in chunks {
            self.file
                .seek(SeekFrom::Start(self.data_start + chunk.offset))?;
            let mut compressed = vec![0; chunk.length as usize];
            self.file.read_exact(&mut compressed)?;
            let mut text = String::new();
            GzDecoder::new(&compressed[..]).read_to_string(&mut text)?;
            for line in text.lines() {
                f(GzbRow::parse_tsv(line)?)?;
                n += 1;
                if max > 0 && n >= max {
                    return Ok(n);
                }
            }
        }
        Ok(n)
    }
}

/// `<root>/<YYYYMM>/<group_id>.gzb`. The PHP API builds the same path.
pub fn gzb_path(root: &Path, group_id: usize, ym: &YearMonth) -> PathBuf {
    root.join(year_month_dir(ym))
        .join(format!("{group_id}.{FILE_EXTENSION}"))
}

pub fn year_month_dir(ym: &YearMonth) -> String {
    format!("{}{:02}", ym.year(), ym.month())
}

/// Key for a page in the pageview dump: `(wiki code, underscored title)`
/// hashed to 64 bits. Holding ~25M of these as integers instead of strings is
/// what lets a whole month's lookup table fit in memory. FNV-1a plus a
/// splitmix64 finalizer — deterministic across builds, unlike std's hasher,
/// because the table is persisted between runs. A collision only means one
/// page picks up another page's views. At ~25M keys and ~440M dump lines a
/// month, that happens about once in 1,700 months (an unrelated dump title
/// hitting a key); two pages sharing a key, about once in 60,000.
pub fn page_key(wiki_code: &[u8], title: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut h = FNV_OFFSET;
    for &b in wiki_code.iter().chain(std::iter::once(&0u8)).chain(title) {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

/// Monthly views for every page in a month's page lists, keyed by
/// [`page_key`]. Sorted keys plus an index of where each bucket of the top
/// key bits starts: 12 bytes per page (~300 MB for 25M pages), against
/// 25-35 for a `HashMap` — more while it doubles. Keys are uniform hashes,
/// so a lookup scans a bucket of about four keys.
pub struct ViewTable {
    keys: Vec<u64>,
    views: Vec<u32>,
    bucket_starts: Vec<u32>,
    shift: u32,
}

impl ViewTable {
    /// All views zero. `keys` may contain duplicates.
    pub fn from_keys(mut keys: Vec<u64>) -> Self {
        keys.sort_unstable();
        keys.dedup();
        keys.shrink_to_fit();
        let views = vec![0; keys.len()];
        Self::indexed(keys, views)
    }

    /// From `(key, views)`; the first entry wins for duplicate keys.
    pub fn from_pairs(mut pairs: Vec<(u64, u32)>) -> Self {
        pairs.sort_by_key(|p| p.0);
        pairs.dedup_by_key(|p| p.0);
        let (keys, views) = pairs.into_iter().unzip();
        Self::indexed(keys, views)
    }

    fn indexed(keys: Vec<u64>, views: Vec<u32>) -> Self {
        let bits = (keys.len() / 4)
            .max(1)
            .next_power_of_two()
            .trailing_zeros()
            .clamp(1, 28);
        let shift = 64 - bits;
        let buckets = 1usize << bits;
        let mut bucket_starts = Vec::with_capacity(buckets + 1);
        let mut i = 0;
        for bucket in 0..=buckets {
            while i < keys.len() && ((keys[i] >> shift) as usize) < bucket {
                i += 1;
            }
            bucket_starts.push(i as u32);
        }
        Self {
            keys,
            views,
            bucket_starts,
            shift,
        }
    }

    fn position(&self, key: u64) -> Option<usize> {
        let bucket = (key >> self.shift) as usize;
        let start = self.bucket_starts[bucket] as usize;
        let end = self.bucket_starts[bucket + 1] as usize;
        self.keys[start..end]
            .iter()
            .position(|k| *k == key)
            .map(|p| start + p)
    }

    /// Views of a page; 0 if unknown.
    pub fn get(&self, key: u64) -> u32 {
        self.position(key).map_or(0, |i| self.views[i])
    }

    /// Add views to a known page. Returns false if the key is unknown.
    pub fn add(&mut self, key: u64, views: u64) -> bool {
        match self.position(key) {
            Some(i) => {
                let v = u32::try_from(views).unwrap_or(u32::MAX);
                self.views[i] = self.views[i].saturating_add(v);
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// `(key, views)` for every page with views.
    pub fn with_views(&self) -> impl Iterator<Item = (u64, u32)> + '_ {
        self.keys
            .iter()
            .copied()
            .zip(self.views.iter().copied())
            .filter(|(_, v)| *v > 0)
    }
}

/// Clamp a view total to what `group_status.total_views` can hold.
pub fn db_views(views: u64) -> u64 {
    if views > MAX_DB_VIEWS {
        log::warn!("total views {views} exceed group_status.total_views; clamping");
        MAX_DB_VIEWS
    } else {
        views
    }
}

/// Ensure `group_status.storage` accepts [`STORAGE`]. Appending an ENUM value
/// is a metadata-only change on InnoDB, so this is cheap and idempotent.
pub async fn ensure_storage_enum(baglama: &crate::Baglama2) -> Result<()> {
    use mysql_async::prelude::*;
    let mut conn = baglama.get_tooldb_conn().await?;
    let column_type: Option<String> = conn
        .query_first(
            "SELECT COLUMN_TYPE FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='group_status' AND COLUMN_NAME='storage'",
        )
        .await?;
    let column_type = column_type.ok_or_else(|| anyhow!("group_status.storage not found"))?;
    if column_type.contains(&format!("'{STORAGE}'")) {
        return Ok(());
    }
    let expected = "enum('file','mysql','sqlite3','mysql2')";
    if column_type != expected {
        return Err(anyhow!(
            "group_status.storage is {column_type}, expected {expected}; not altering it automatically"
        ));
    }
    log::info!("Adding '{STORAGE}' to group_status.storage");
    conn.query_drop(format!(
        "ALTER TABLE group_status MODIFY `storage` enum('file','mysql','sqlite3','mysql2','{STORAGE}') NOT NULL DEFAULT 'sqlite3'"
    ))
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(title: &str, views: u64, files: &[&str]) -> GzbRow {
        GzbRow {
            title: title.to_string(),
            namespace_id: 0,
            views,
            files: files.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_roundtrip() {
        let dir = std::env::temp_dir().join(format!("gzb_test_{}", std::process::id()));
        let ym = YearMonth::new(2024, 3).unwrap();
        let path = gzb_path(&dir, 42, &ym);
        let mut w = GzbWriter::new(42, &ym, "dump");
        let many: Vec<GzbRow> = (0..CHUNK_ROWS as u64 * 2 + 7)
            .map(|i| row(&format!("Page_{i}"), i, &["B.jpg", "A.jpg", "B.jpg"]))
            .collect();
        w.add_site("enwiki", many, None).unwrap();
        w.add_site(
            "dewiki",
            vec![row("Köln", 5, &[]), row("Bonn", 9, &["X.jpg"])],
            Some((3, 20)),
        )
        .unwrap();
        w.add_site("frwiki", vec![], None).unwrap();
        let written = w.finish(&path).unwrap();

        let mut r = GzbReader::open(&path).unwrap();
        assert_eq!(r.header(), &written);
        assert_eq!(r.header().sites[0].giu, "enwiki");
        assert_eq!(r.header().site("enwiki").unwrap().chunks.len(), 3);
        assert_eq!(r.header().site("dewiki").unwrap().pages, 3);
        assert_eq!(r.header().site("dewiki").unwrap().views, 20);

        let top = r.rows("enwiki", 3).unwrap();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].title, format!("Page_{}", CHUNK_ROWS * 2 + 6));
        assert_eq!(top[0].files, vec!["A.jpg", "B.jpg"]);
        assert_eq!(r.rows("enwiki", 0).unwrap().len(), CHUNK_ROWS * 2 + 7);

        let de = r.rows("dewiki", 0).unwrap();
        assert_eq!(de[0].title, "Bonn");
        assert_eq!(de[1], row("Köln", 5, &[]));
        assert!(r.rows("frwiki", 0).unwrap().is_empty());
        assert!(r.rows("nowiki", 0).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_view_table() {
        let a = page_key(b"en.wikipedia", b"A");
        let b = page_key(b"en.wikipedia", b"B");
        let mut t = ViewTable::from_keys(vec![a, b, a, 0, u64::MAX]);
        assert_eq!(t.len(), 4);
        assert!(t.add(a, 5));
        assert!(t.add(a, u64::MAX)); // saturates
        assert!(!t.add(page_key(b"en.wikipedia", b"C"), 1));
        assert!(t.add(0, 1) && t.add(u64::MAX, 2));
        assert_eq!(t.get(a), u32::MAX);
        assert_eq!(t.get(b), 0);
        assert_eq!(t.get(12345), 0);
        let mut nonzero: Vec<_> = t.with_views().collect();
        nonzero.sort();
        assert_eq!(nonzero, vec![(0, 1), (a, u32::MAX), (u64::MAX, 2)]);
        let back = ViewTable::from_pairs(nonzero);
        assert_eq!((back.get(u64::MAX), back.get(b), back.len()), (2, 0, 3));
        assert!(ViewTable::from_keys(vec![]).is_empty());
        assert_eq!(ViewTable::from_keys(vec![]).get(a), 0);
    }

    #[test]
    fn test_view_table_many() {
        let keys: Vec<u64> = (0..100_000u64)
            .map(|i| page_key(b"x", &i.to_le_bytes()))
            .collect();
        let mut t = ViewTable::from_keys(keys.clone());
        for (i, k) in keys.iter().enumerate() {
            assert!(t.add(*k, i as u64));
        }
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(t.get(*k), i as u32);
        }
    }

    #[test]
    fn test_sanitize_fields() {
        let mut out = vec![];
        row("A\tB", 1, &["x|y.jpg"]).write_tsv(&mut out);
        assert_eq!(String::from_utf8(out).unwrap(), "A B\t0\t1\tx_y.jpg\n");
    }

    #[test]
    fn test_page_key() {
        assert_eq!(
            page_key(b"en.wikipedia", b"Foo"),
            page_key(b"en.wikipedia", b"Foo")
        );
        assert_ne!(
            page_key(b"en.wikipedia", b"Foo"),
            page_key(b"de.wikipedia", b"Foo")
        );
        // The separator keeps (code, title) splits apart.
        assert_ne!(page_key(b"ab", b"c"), page_key(b"a", b"bc"));
        // Pinned: a persisted views table must stay readable by later builds.
        assert_eq!(page_key(b"en.wikipedia", b"Foo"), 0xa807_bd7b_d6b6_06e6);
    }
}
