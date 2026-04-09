//! Wikimedia pageview-complete dump scanner.
//!
//! This module provides tools for reading monthly pageview dumps from
//! <https://dumps.wikimedia.org/other/pageview_complete/monthly/>.
//!
//! The core scanning logic is generic over any `std::io::Read` source and
//! has no dependency on MySQL or any other database.  It is designed to be
//! extractable into a standalone library crate later.

use anyhow::Result;
use log::info;
use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::PathBuf;

/// Accumulated view data for a single wiki code (site).
///
/// When the dump scanner encounters a transition from one wiki code to
/// the next, it emits one of these for the just-finished wiki section.
/// The `title_views` map contains `title → total_views` (summed across
/// access types).
pub struct SiteViewData {
    pub wiki_code: String,
    pub title_views: HashMap<String, u64>,
}

/// Two-level lookup: wiki_code → (title → Vec<pages_id>).
///
/// The first level allows the hot scanning loop to test whether a wiki
/// code is relevant with a single `HashMap::get` on a borrowed `&str` —
/// zero allocation for the >99% of lines whose wiki code we don't need.
/// Only lines matching a known wiki proceed to the second-level title
/// lookup (also zero-alloc via `&str`).
pub type DumpLookup = HashMap<String, HashMap<String, Vec<usize>>>;

/// Set of wiki codes (e.g. `"en.wikipedia"`) that the scanner should
/// pay attention to.  Lines for wikis not in this set are skipped with
/// zero allocation.
pub type WikiFilter = HashSet<String>;

/// Result of scanning a dump file: a map from pages_id to its total
/// view count, and the set of pages_ids that were not found in the dump
/// (i.e. zero views that month).
pub struct DumpScanResult {
    pub id2views: HashMap<usize, u64>,
    pub unmatched_ids: HashSet<usize>,
}

/// Capacity of the bounded byte-channel used to pipe an HTTP download
/// stream into the blocking decompression thread.
const DOWNLOAD_CHANNEL_CAPACITY: usize = 256;

/// Well-known Toolforge path where Wikimedia dumps are mirrored locally.
const TOOLFORGE_DUMP_ROOT: &str = "/public/dumps";

// -----------------------------------------------------------------------
// ChannelReader: sync Read adapter backed by an mpsc channel
// -----------------------------------------------------------------------

/// Adapter that implements `std::io::Read` by pulling `bytes::Bytes`
/// chunks from a `std::sync::mpsc::Receiver`.  This bridges an async
/// HTTP download (which sends chunks into the channel) with the
/// synchronous `BzDecoder` running on a blocking thread.
pub(crate) struct ChannelReader {
    rx: std::sync::mpsc::Receiver<bytes::Bytes>,
    /// Leftover bytes from the last chunk that weren't fully consumed.
    buf: bytes::Bytes,
}

impl ChannelReader {
    pub fn new(rx: std::sync::mpsc::Receiver<bytes::Bytes>) -> Self {
        Self {
            rx,
            buf: bytes::Bytes::new(),
        }
    }
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, dest: &mut [u8]) -> std::io::Result<usize> {
        if !self.buf.is_empty() {
            let n = std::cmp::min(dest.len(), self.buf.len());
            dest[..n].copy_from_slice(&self.buf[..n]);
            self.buf = self.buf.slice(n..);
            return Ok(n);
        }
        match self.rx.recv() {
            Ok(chunk) => {
                let n = std::cmp::min(dest.len(), chunk.len());
                dest[..n].copy_from_slice(&chunk[..n]);
                if n < chunk.len() {
                    self.buf = chunk.slice(n..);
                }
                Ok(n)
            }
            Err(_) => Ok(0), // channel closed → EOF
        }
    }
}

// -----------------------------------------------------------------------
// Lookup builder
// -----------------------------------------------------------------------

/// Build a two-level `DumpLookup` from raw DB rows.
///
/// Each row is `(pages_id, server, title)` where `server` is like
/// `"en.wikipedia.org"`.  The trailing `".org"` is stripped to form the
/// wiki code used in the dump files.  Spaces in titles are replaced with
/// underscores to match the dump format.
///
/// Returns `(lookup, all_ids)`.
pub fn build_dump_lookup(rows: Vec<(usize, String, String)>) -> (DumpLookup, HashSet<usize>) {
    let mut lookup: DumpLookup = HashMap::new();
    let mut all_ids: HashSet<usize> = HashSet::with_capacity(rows.len());

    for (pages_id, server, title) in rows {
        if title.is_empty() {
            continue;
        }
        let wiki_code = match server.strip_suffix(".org") {
            Some(w) => w,
            None => continue,
        };
        let title_underscored = title.replace(' ', "_");
        lookup
            .entry(wiki_code.to_owned())
            .or_default()
            .entry(title_underscored)
            .or_default()
            .push(pages_id);
        all_ids.insert(pages_id);
    }
    (lookup, all_ids)
}

// -----------------------------------------------------------------------
// URL / path helpers
// -----------------------------------------------------------------------

/// URL of the monthly pageview-complete dump for human ("user") traffic.
pub fn dump_url(year: i32, month: u32) -> String {
    format!(
        "https://dumps.wikimedia.org/other/pageview_complete/\
         monthly/{year}/{year}-{month:02}/pageviews-{year}{month:02}-user.bz2"
    )
}

/// Return the local filesystem path to the dump if it is mirrored on
/// this host (e.g. Toolforge's `/public/dumps/`).  Returns `None` if
/// the file doesn't exist locally.
pub fn local_dump_path(year: i32, month: u32) -> Option<PathBuf> {
    let path = PathBuf::from(format!(
        "{TOOLFORGE_DUMP_ROOT}/other/pageview_complete/\
         monthly/{year}/{year}-{month:02}/pageviews-{year}{month:02}-user.bz2"
    ));
    if path.is_file() {
        Some(path)
    } else {
        None
    }
}

// -----------------------------------------------------------------------
// Scan orchestrators — streaming per-site callback variant
// -----------------------------------------------------------------------

/// Open a local dump file and stream it through `scan_dump_by_site`.
///
/// Runs on a blocking thread.  Calls `site_callback` once per wiki code
/// section found in the dump.
pub async fn stream_local_file_by_site<F>(
    path: &PathBuf,
    wiki_filter: WikiFilter,
    site_callback: F,
) -> Result<()>
where
    F: FnMut(SiteViewData) + Send + 'static,
{
    let path = path.clone();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::with_capacity(512 * 1024, file);
        scan_dump_by_site(reader, &wiki_filter, site_callback)
    })
    .await?
}

/// Stream-download the dump over HTTP and run `scan_dump_by_site`.
///
/// Pipes response chunks through a bounded channel into the blocking
/// decompression thread, overlapping network I/O with decompression.
/// Calls `site_callback` once per wiki code section.
pub async fn stream_http_by_site<F>(
    dump_url: &str,
    wiki_filter: WikiFilter,
    site_callback: F,
) -> Result<()>
where
    F: FnMut(SiteViewData) + Send + 'static,
{
    use anyhow::anyhow;
    use futures::StreamExt;

    let response = reqwest::get(dump_url).await?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Dump download failed: HTTP {} for {}",
            response.status(),
            dump_url
        ));
    }

    let (tx, rx) = std::sync::mpsc::sync_channel::<bytes::Bytes>(DOWNLOAD_CHANNEL_CAPACITY);

    let scan_handle = tokio::task::spawn_blocking(move || {
        let reader = ChannelReader::new(rx);
        let buf_reader = std::io::BufReader::with_capacity(512 * 1024, reader);
        scan_dump_by_site(buf_reader, &wiki_filter, site_callback)
    });

    let mut stream = response.bytes_stream();
    let mut downloaded_bytes: u64 = 0;
    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => {
                downloaded_bytes += chunk.len() as u64;
                if tx.send(chunk).is_err() {
                    break;
                }
            }
            Err(e) => {
                drop(tx);
                return Err(anyhow!(
                    "HTTP stream error after {} MiB: {}",
                    downloaded_bytes / (1024 * 1024),
                    e
                ));
            }
        }
    }
    drop(tx);
    info!(
        "dump download complete ({} MiB), waiting for scanner…",
        downloaded_bytes / (1024 * 1024)
    );

    scan_handle.await?
}

// -----------------------------------------------------------------------
// Scan orchestrators — old pre-load-everything variant (kept for tests)
// -----------------------------------------------------------------------

/// Scan the dump from a local file.  Runs the bz2 decompression +
/// line scan on a blocking thread to avoid starving the tokio runtime.
pub async fn scan_dump_from_local_file(
    path: &PathBuf,
    lookup: DumpLookup,
    all_ids: HashSet<usize>,
) -> Result<DumpScanResult> {
    let path = path.clone();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::with_capacity(512 * 1024, file);
        scan_dump_reader(reader, &lookup, &all_ids)
    })
    .await?
}

/// Stream-download the dump over HTTP and decompress + scan it
/// concurrently on a blocking thread.
pub async fn scan_dump_from_http(
    dump_url: &str,
    lookup: DumpLookup,
    all_ids: HashSet<usize>,
) -> Result<DumpScanResult> {
    use anyhow::anyhow;
    use futures::StreamExt;

    let response = reqwest::get(dump_url).await?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Dump download failed: HTTP {} for {}",
            response.status(),
            dump_url
        ));
    }

    let (tx, rx) = std::sync::mpsc::sync_channel::<bytes::Bytes>(DOWNLOAD_CHANNEL_CAPACITY);

    let scan_handle = tokio::task::spawn_blocking(move || {
        let reader = ChannelReader::new(rx);
        let buf_reader = std::io::BufReader::with_capacity(512 * 1024, reader);
        scan_dump_reader(buf_reader, &lookup, &all_ids)
    });

    let mut stream = response.bytes_stream();
    let mut downloaded_bytes: u64 = 0;
    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => {
                downloaded_bytes += chunk.len() as u64;
                if tx.send(chunk).is_err() {
                    break;
                }
            }
            Err(e) => {
                drop(tx);
                return Err(anyhow!(
                    "HTTP stream error after {} MiB: {}",
                    downloaded_bytes / (1024 * 1024),
                    e
                ));
            }
        }
    }
    drop(tx);
    info!(
        "dump download complete ({} MiB), waiting for scanner…",
        downloaded_bytes / (1024 * 1024)
    );

    scan_handle.await?
}

// -----------------------------------------------------------------------
// Core scanner
// -----------------------------------------------------------------------

/// Streaming per-site scanner.
///
/// Reads the bz2-compressed dump line by line.  Because the dump is
/// sorted by wiki code, all lines for a given wiki appear consecutively.
/// The scanner accumulates `title → total_views` for the current wiki
/// section, and when the wiki code changes (or at EOF), it calls
/// `site_callback` with the completed [`SiteViewData`] — but only if
/// the wiki code is in `wiki_filter`.
///
/// This design keeps memory proportional to a *single* wiki's worth of
/// pages at a time (typically a few hundred MB for the largest wikis)
/// rather than the entire dump or the entire viewdata table.
pub fn scan_dump_by_site<R: std::io::Read, F>(
    raw_reader: std::io::BufReader<R>,
    wiki_filter: &WikiFilter,
    mut site_callback: F,
) -> Result<()>
where
    F: FnMut(SiteViewData),
{
    use bzip2::read::BzDecoder;

    let decompressor = BzDecoder::new(raw_reader);
    let mut buf_reader = std::io::BufReader::with_capacity(256 * 1024, decompressor);

    let mut lines_scanned: u64 = 0;
    let mut matched_count: u64 = 0;
    let mut sites_emitted: u64 = 0;
    let mut line_buf = String::with_capacity(512);

    // State for the current wiki section.
    let mut current_wiki: Option<String> = None;
    let mut current_titles: HashMap<String, u64> = HashMap::new();

    loop {
        line_buf.clear();
        match buf_reader.read_line(&mut line_buf) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                if lines_scanned > 0 {
                    eprintln!(
                        "scan_dump_by_site: I/O error at line {}: {}",
                        lines_scanned, e
                    );
                }
                continue;
            }
        }
        lines_scanned += 1;
        if lines_scanned % 50_000_000 == 0 {
            eprintln!(
                "scan_dump_by_site: scanned {} M lines, {} matches, {} sites emitted",
                lines_scanned / 1_000_000,
                matched_count,
                sites_emitted,
            );
        }

        let line = line_buf.trim_end_matches('\n').trim_end_matches('\r');

        // Format: wiki_code  title  page_id  access_type  monthly_total  hourly
        let mut cols = line.splitn(6, ' ');
        let wiki_code = match cols.next() {
            Some(w) if !w.is_empty() => w,
            _ => continue,
        };

        // Detect wiki-code transitions.  When the wiki changes, emit the
        // accumulated data for the previous wiki (if any) and reset.
        let wiki_changed = match &current_wiki {
            Some(cw) => cw.as_str() != wiki_code,
            None => true,
        };
        if wiki_changed {
            // Flush the previous wiki's data.
            if let Some(prev_wiki) = current_wiki.take() {
                if !current_titles.is_empty() {
                    sites_emitted += 1;
                    site_callback(SiteViewData {
                        wiki_code: prev_wiki,
                        title_views: std::mem::take(&mut current_titles),
                    });
                }
            }
            // Start tracking the new wiki (only if we care about it).
            if wiki_filter.contains(wiki_code) {
                current_wiki = Some(wiki_code.to_owned());
            } else {
                current_wiki = None;
            }
        }

        // Skip lines for wikis we don't care about.
        if current_wiki.is_none() {
            continue;
        }

        let title = match cols.next() {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };

        // Skip page_id (col 2) and access_type (col 3).
        let _page_id = cols.next();
        let _access = cols.next();
        let monthly_total: u64 = match cols.next().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };

        // Sum across access types (desktop + mobile-web + mobile-app).
        matched_count += 1;
        *current_titles.entry(title.to_owned()).or_insert(0) += monthly_total;
    }

    // Flush the last wiki section.
    if let Some(last_wiki) = current_wiki.take() {
        if !current_titles.is_empty() {
            sites_emitted += 1;
            site_callback(SiteViewData {
                wiki_code: last_wiki,
                title_views: current_titles,
            });
        }
    }

    eprintln!(
        "scan_dump_by_site: finished scanning {} lines, \
         {} matching rows, {} sites emitted",
        lines_scanned, matched_count, sites_emitted,
    );

    Ok(())
}

// -----------------------------------------------------------------------
// Old pre-load-everything scanner (used by existing tests and as fallback)
// -----------------------------------------------------------------------

/// Core scanning logic that pre-loads all lookups into memory.
///
/// Accepts any `Read` source (wraps it in `BzDecoder`).
/// Uses the two-level `DumpLookup` and `read_line` with a reusable buffer.
///
/// Prefer [`scan_dump_by_site`] for production use with large tables.
pub fn scan_dump_reader<R: std::io::Read>(
    raw_reader: std::io::BufReader<R>,
    lookup: &DumpLookup,
    all_ids: &HashSet<usize>,
) -> Result<DumpScanResult> {
    use bzip2::read::BzDecoder;

    let decompressor = BzDecoder::new(raw_reader);
    let mut buf_reader = std::io::BufReader::with_capacity(256 * 1024, decompressor);

    let mut id2views: HashMap<usize, u64> = HashMap::new();
    let mut lines_scanned: u64 = 0;
    let mut matched_count: u64 = 0;
    let mut line_buf = String::with_capacity(512);

    loop {
        line_buf.clear();
        match buf_reader.read_line(&mut line_buf) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                if lines_scanned > 0 {
                    eprintln!(
                        "scan_dump_reader: I/O error at line {}: {}",
                        lines_scanned, e
                    );
                }
                continue;
            }
        }
        lines_scanned += 1;
        if lines_scanned % 50_000_000 == 0 {
            eprintln!(
                "scan_dump_reader: scanned {} M lines, {} matches so far",
                lines_scanned / 1_000_000,
                matched_count
            );
        }

        let line = line_buf.trim_end_matches('\n').trim_end_matches('\r');

        let mut cols = line.splitn(6, ' ');
        let wiki_code = match cols.next() {
            Some(w) if !w.is_empty() => w,
            _ => continue,
        };

        let title_map = match lookup.get(wiki_code) {
            Some(m) => m,
            None => continue,
        };

        let title = match cols.next() {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };

        let pages_ids = match title_map.get(title) {
            Some(ids) => ids,
            None => continue,
        };

        let _page_id = cols.next();
        let _access = cols.next();
        let monthly_total: u64 = match cols.next().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };

        matched_count += 1;
        for &pid in pages_ids {
            *id2views.entry(pid).or_insert(0) += monthly_total;
        }
    }

    eprintln!(
        "scan_dump_reader: finished scanning {} lines, \
         {} matching rows, {} unique pages_ids with views",
        lines_scanned,
        matched_count,
        id2views.len()
    );

    let unmatched_ids: HashSet<usize> = all_ids
        .iter()
        .filter(|id| !id2views.contains_key(id))
        .copied()
        .collect();

    Ok(DumpScanResult {
        id2views,
        unmatched_ids,
    })
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ----- helpers -------------------------------------------------------

    /// Compress text with bz2 and run scan_dump_reader against it.
    fn compress_and_scan(
        dump_text: &str,
        lookup: &DumpLookup,
        all_ids: &HashSet<usize>,
    ) -> DumpScanResult {
        use bzip2::write::BzEncoder;
        use bzip2::Compression;
        use std::io::Write;

        let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(dump_text.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let buf_reader = std::io::BufReader::new(compressed.as_slice());
        scan_dump_reader(buf_reader, lookup, all_ids).unwrap()
    }

    /// Compress text with bz2 and run scan_dump_by_site, collecting
    /// emitted SiteViewData into a Vec.
    fn compress_and_scan_by_site(dump_text: &str, wiki_filter: &WikiFilter) -> Vec<SiteViewData> {
        use bzip2::write::BzEncoder;
        use bzip2::Compression;
        use std::io::Write;

        let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(dump_text.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let buf_reader = std::io::BufReader::new(compressed.as_slice());
        let mut results = Vec::new();
        scan_dump_by_site(buf_reader, wiki_filter, |svd| {
            results.push(svd);
        })
        .unwrap();
        results
    }

    // ----- scan_dump_reader tests (pre-load variant) ---------------------

    #[test]
    fn test_scan_dump_reader_basic() {
        let dump_text = "\
en.wikipedia Barack_Obama null desktop 100 A100\n\
en.wikipedia Barack_Obama null mobile-web 50 A50\n\
de.wikipedia Trude_Herr null desktop 30 A30\n\
fr.wikipedia Unrelated_Page null desktop 999 A999\n";

        let mut lookup: DumpLookup = HashMap::new();
        lookup
            .entry("en.wikipedia".into())
            .or_default()
            .insert("Barack_Obama".into(), vec![1, 2]);
        lookup
            .entry("de.wikipedia".into())
            .or_default()
            .insert("Trude_Herr".into(), vec![3]);

        let all_ids: HashSet<usize> = [1, 2, 3, 4].into_iter().collect();
        let result = compress_and_scan(dump_text, &lookup, &all_ids);

        assert_eq!(result.id2views.get(&1), Some(&150));
        assert_eq!(result.id2views.get(&2), Some(&150));
        assert_eq!(result.id2views.get(&3), Some(&30));
        assert!(result.unmatched_ids.contains(&4));
        assert_eq!(result.unmatched_ids.len(), 1);
    }

    #[test]
    fn test_scan_dump_reader_empty_lookup() {
        let dump_text = "en.wikipedia Some_Page null desktop 42 A42\n";
        let lookup: DumpLookup = HashMap::new();
        let all_ids: HashSet<usize> = [10].into_iter().collect();
        let result = compress_and_scan(dump_text, &lookup, &all_ids);

        assert!(result.id2views.is_empty());
        assert_eq!(result.unmatched_ids.len(), 1);
        assert!(result.unmatched_ids.contains(&10));
    }

    #[test]
    fn test_scan_dump_two_level_selectivity() {
        let mut lookup: DumpLookup = HashMap::new();
        lookup
            .entry("en.wikipedia".into())
            .or_default()
            .insert("Page_A".into(), vec![1]);

        let all_ids: HashSet<usize> = [1].into_iter().collect();

        let dump_text = "\
en.wikipedia Page_A null desktop 10 A10\n\
en.wikipedia Page_B null desktop 20 A20\n\
de.wikipedia Page_A null desktop 30 A30\n";

        let result = compress_and_scan(dump_text, &lookup, &all_ids);
        assert_eq!(result.id2views.get(&1), Some(&10));
        assert!(result.unmatched_ids.is_empty());
    }

    // ----- scan_dump_by_site tests (streaming per-site variant) ----------

    #[test]
    fn test_scan_by_site_basic() {
        let dump_text = "\
de.wikipedia Trude_Herr null desktop 30 A30\n\
en.wikipedia Barack_Obama null desktop 100 A100\n\
en.wikipedia Barack_Obama null mobile-web 50 A50\n\
en.wikipedia Other_Page null desktop 7 A7\n\
fr.wikipedia Ignored null desktop 999 A999\n";

        let filter: WikiFilter = ["en.wikipedia".into(), "de.wikipedia".into()]
            .into_iter()
            .collect();
        let sites = compress_and_scan_by_site(dump_text, &filter);

        // Two sites emitted (de before en because the dump is sorted).
        assert_eq!(sites.len(), 2);

        assert_eq!(sites[0].wiki_code, "de.wikipedia");
        assert_eq!(sites[0].title_views.get("Trude_Herr"), Some(&30));
        assert_eq!(sites[0].title_views.len(), 1);

        assert_eq!(sites[1].wiki_code, "en.wikipedia");
        // Barack_Obama: desktop(100) + mobile-web(50) = 150
        assert_eq!(sites[1].title_views.get("Barack_Obama"), Some(&150));
        assert_eq!(sites[1].title_views.get("Other_Page"), Some(&7));
        assert_eq!(sites[1].title_views.len(), 2);
    }

    #[test]
    fn test_scan_by_site_skips_unfiltered_wikis() {
        let dump_text = "\
aa.wikipedia Page null desktop 1 A1\n\
en.wikipedia Page null desktop 2 A2\n\
zz.wikipedia Page null desktop 3 A3\n";

        // Only care about en.wikipedia.
        let filter: WikiFilter = ["en.wikipedia".into()].into_iter().collect();
        let sites = compress_and_scan_by_site(dump_text, &filter);

        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].wiki_code, "en.wikipedia");
        assert_eq!(sites[0].title_views.get("Page"), Some(&2));
    }

    #[test]
    fn test_scan_by_site_empty_filter() {
        let dump_text = "en.wikipedia Page null desktop 42 A42\n";
        let filter: WikiFilter = HashSet::new();
        let sites = compress_and_scan_by_site(dump_text, &filter);
        assert!(sites.is_empty());
    }

    #[test]
    fn test_scan_by_site_sums_access_types() {
        let dump_text = "\
en.wikipedia Page null desktop 10 A10\n\
en.wikipedia Page null mobile-web 20 A20\n\
en.wikipedia Page null mobile-app 5 A5\n";

        let filter: WikiFilter = ["en.wikipedia".into()].into_iter().collect();
        let sites = compress_and_scan_by_site(dump_text, &filter);

        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].title_views.get("Page"), Some(&35));
    }

    // ----- other tests ---------------------------------------------------

    #[test]
    fn test_build_dump_lookup() {
        let rows = vec![
            (1, "en.wikipedia.org".to_string(), "Foo Bar".to_string()),
            (2, "en.wikipedia.org".to_string(), "Foo Bar".to_string()),
            (3, "de.wikipedia.org".to_string(), "Baz".to_string()),
            (4, "bad-no-dot-org".to_string(), "Ignored".to_string()),
            (5, "en.wikipedia.org".to_string(), "".to_string()),
        ];
        let (lookup, all_ids) = build_dump_lookup(rows);

        assert_eq!(all_ids.len(), 3);
        assert!(all_ids.contains(&1));
        assert!(all_ids.contains(&2));
        assert!(all_ids.contains(&3));
        assert_eq!(lookup.len(), 2);

        let en = lookup.get("en.wikipedia").unwrap();
        assert_eq!(en.get("Foo_Bar").unwrap(), &vec![1, 2]);

        let de = lookup.get("de.wikipedia").unwrap();
        assert_eq!(de.get("Baz").unwrap(), &vec![3]);
    }

    #[test]
    fn test_channel_reader() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<bytes::Bytes>(4);
        tx.send(bytes::Bytes::from_static(b"hello ")).unwrap();
        tx.send(bytes::Bytes::from_static(b"world")).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut out = String::new();
        std::io::Read::read_to_string(&mut reader, &mut out).unwrap();
        assert_eq!(out, "hello world");
    }

    #[test]
    fn test_dump_url_format() {
        assert_eq!(
            dump_url(2025, 1),
            "https://dumps.wikimedia.org/other/pageview_complete/monthly/2025/2025-01/pageviews-202501-user.bz2"
        );
    }
}
