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

/// Two-level lookup: wiki_code → (title → Vec<pages_id>).
///
/// The first level allows the hot scanning loop to test whether a wiki
/// code is relevant with a single `HashMap::get` on a borrowed `&str` —
/// zero allocation for the >99% of lines whose wiki code we don't need.
/// Only lines matching a known wiki proceed to the second-level title
/// lookup (also zero-alloc via `&str`).
pub type DumpLookup = HashMap<String, HashMap<String, Vec<usize>>>;

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
// Scan orchestrators
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
///
/// Instead of downloading the entire ~4 GB file into memory first,
/// we pipe response chunks through a bounded `sync_channel` into the
/// blocking decompression thread.  This overlaps network I/O with
/// CPU-bound decompression so that wall-clock time ≈
/// `max(download_time, decompress_time)` rather than their sum, and
/// peak memory usage drops from ~4 GB to a small channel buffer.
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
        "load_views_from_dump: download complete ({} MiB), waiting for scanner…",
        downloaded_bytes / (1024 * 1024)
    );

    scan_handle.await?
}

// -----------------------------------------------------------------------
// Core scanner
// -----------------------------------------------------------------------

/// Core scanning logic shared by the local-file and streaming-HTTP paths.
///
/// Accepts any `Read` source (wraps it in `BzDecoder`).
///
/// Uses a **two-level lookup** (`wiki → title → ids`) so that lines
/// whose wiki code is not in our lookup are rejected with a single
/// `HashMap::get` on a borrowed `&str` — zero allocation for the vast
/// majority of the ~400 M lines in the dump.
///
/// Uses `read_line` into a **reusable `String` buffer** instead of
/// the `.lines()` iterator, avoiding a fresh allocation per line.
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
        if lines_scanned.is_multiple_of(50_000_000) {
            eprintln!(
                "scan_dump_reader: scanned {} M lines, {} matches so far",
                lines_scanned / 1_000_000,
                matched_count
            );
        }

        let line = line_buf.trim_end_matches('\n').trim_end_matches('\r');

        // Format: wiki_code  title  page_id  access_type  monthly_total  hourly
        let mut cols = line.splitn(6, ' ');
        let wiki_code = match cols.next() {
            Some(w) if !w.is_empty() => w,
            _ => continue,
        };

        // First-level check: do we care about this wiki at all?
        let title_map = match lookup.get(wiki_code) {
            Some(m) => m,
            None => continue,
        };

        let title = match cols.next() {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };

        // Second-level check: do we care about this title?
        let pages_ids = match title_map.get(title) {
            Some(ids) => ids,
            None => continue,
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

    /// Helper: compress text and run scan_dump_reader against it.
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
