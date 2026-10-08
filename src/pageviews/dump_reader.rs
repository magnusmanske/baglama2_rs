//! Wikimedia pageview-complete dump scanner.
//!
//! Reads the monthly pageview dumps from
//! <https://dumps.wikimedia.org/other/pageview_complete/monthly/>, as
//! mirrored on Toolforge. Generic over any `std::io::Read` source and with
//! no database dependency.

use anyhow::Result;
use log::info;
use std::io::BufRead;
use std::path::PathBuf;

/// Well-known Toolforge path where Wikimedia dumps are mirrored locally.
const TOOLFORGE_DUMP_ROOT: &str = "/public/dumps";

/// Where the `-user` dump for a month lives on Toolforge's dumps mount,
/// whether or not it exists (yet).
pub fn local_dump_path_unchecked(year: i32, month: u32) -> PathBuf {
    PathBuf::from(format!(
        "{TOOLFORGE_DUMP_ROOT}/public/other/pageview_complete/\
         monthly/{year}/{year}-{month:02}/pageviews-{year}{month:02}-user.bz2"
    ))
}

/// Stream every line of a monthly pageview dump as raw bytes, calling
/// `f(wiki_code, title, monthly_total)`. Returns the number of lines read.
///
/// Allocates nothing per line, does not require valid UTF-8, and reads
/// multi-stream bzip2 to the end — `BzDecoder` stops silently after the
/// first stream.
pub fn scan_dump_lines<R, F>(raw: R, mut f: F) -> Result<u64>
where
    R: std::io::Read,
    F: FnMut(&[u8], &[u8], u64),
{
    use bzip2::read::MultiBzDecoder;
    let mut reader = std::io::BufReader::with_capacity(1024 * 1024, MultiBzDecoder::new(raw));
    let mut line = Vec::with_capacity(1024);
    let mut lines: u64 = 0;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        lines += 1;
        if lines.is_multiple_of(100_000_000) {
            info!("scan_dump_lines: {}M lines", lines / 1_000_000);
        }
        if let Some((code, title, views)) = parse_dump_line(&line) {
            f(code, title, views);
        }
    }
    info!("scan_dump_lines: done, {lines} lines");
    Ok(lines)
}

/// `wiki_code title page_id access_type monthly_total hourly` →
/// `(wiki_code, title, monthly_total)`.
fn parse_dump_line(line: &[u8]) -> Option<(&[u8], &[u8], u64)> {
    let mut cols = line.splitn(6, |&b| b == b' ');
    let code = cols.next().filter(|c| !c.is_empty())?;
    let title = cols.next().filter(|t| !t.is_empty())?;
    let _page_id = cols.next()?;
    let _access = cols.next()?;
    let views = cols.next()?;
    let views = std::str::from_utf8(views).ok()?.trim_end().parse().ok()?;
    Some((code, title, views))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dump_line() {
        assert_eq!(
            parse_dump_line(b"en.wikipedia Foo_bar 123 desktop 42 A1B41\n"),
            Some((&b"en.wikipedia"[..], &b"Foo_bar"[..], 42))
        );
        assert_eq!(
            parse_dump_line(b"de.wikipedia Kategorie:X null mobile-web 7 S7\n"),
            Some((&b"de.wikipedia"[..], &b"Kategorie:X"[..], 7))
        );
        assert_eq!(parse_dump_line(b"en.wikipedia Foo 1 desktop\n"), None);
        assert_eq!(parse_dump_line(b"\n"), None);
    }

    #[test]
    fn test_scan_dump_lines_multistream() {
        use bzip2::write::BzEncoder;
        use std::io::Write;
        let mut data = vec![];
        for part in [
            &b"a.wikipedia X 1 desktop 2 B2\n"[..],
            b"b.wikipedia Y 2 desktop 3 C3\n",
        ] {
            let mut enc = BzEncoder::new(Vec::new(), bzip2::Compression::fast());
            enc.write_all(part).unwrap();
            data.extend(enc.finish().unwrap());
        }
        let mut seen = vec![];
        let n =
            scan_dump_lines(&data[..], |c, t, v| seen.push((c.to_vec(), t.to_vec(), v))).unwrap();
        assert_eq!(n, 2);
        assert_eq!(seen[1], (b"b.wikipedia".to_vec(), b"Y".to_vec(), 3));
    }
}
