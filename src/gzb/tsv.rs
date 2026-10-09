//! `gzb_tsv`: export one gzb file as tab-separated text.
//!
//! `#` comment lines with metadata, one header line, then one row per page:
//! `wiki \t title \t namespace \t views \t files`, wikis in header order
//! (most views first), pages by views. Streams chunk by chunk, so even the
//! largest group is never held in memory at once.

use super::*;

pub const COLUMNS: &str = "wiki\ttitle\tnamespace\tviews\tfiles";

/// What the `#` lines say beyond the file's own header.
#[derive(Debug, Default)]
pub struct TsvMeta {
    /// What the group tracks, e.g. `Category:NASA (depth 5)`.
    pub group_label: Option<String>,
    /// Export only this wiki.
    pub wiki: Option<String>,
}

/// Write `reader`'s data to `out`. Returns the number of page rows.
pub fn export(reader: &mut GzbReader, meta: &TsvMeta, out: &mut impl Write) -> Result<u64> {
    let h = reader.header().clone();
    let sites: Vec<&GzbSiteHeader> = match &meta.wiki {
        Some(wiki) => vec![h
            .site(wiki)
            .ok_or_else(|| anyhow!("{wiki} has no pages in this file; see gzb_show"))?],
        None => h.sites.iter().collect(),
    };

    writeln!(
        out,
        "# BaGLAMa 2 page views, {}-{:02}; exported {} by baglama2 v{}",
        h.year,
        h.month,
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        env!("CARGO_PKG_VERSION")
    )?;
    match &meta.group_label {
        Some(label) => writeln!(out, "# group: {} — {label}", h.group_id)?,
        None => writeln!(out, "# group: {}", h.group_id)?,
    }
    writeln!(
        out,
        "# source: {} (data file created {})",
        h.source, h.created
    )?;
    writeln!(
        out,
        "# totals: {} pages, {} views on {} wikis{}",
        h.total_pages,
        h.total_views,
        h.sites.len(),
        rows_note(h.total_pages, h.sites.iter().map(|s| s.rows()).sum())
    )?;
    if let (Some(wiki), Some(site)) = (&meta.wiki, sites.first()) {
        writeln!(
            out,
            "# this export: {wiki} only, {} pages, {} views{}",
            site.pages,
            site.views,
            rows_note(site.pages, site.rows())
        )?;
    }
    if h.source == "dump" {
        writeln!(
            out,
            "# views: monthly page views by users (no bots or spiders), all platforms, \
             from the Wikimedia pageview dump; Main_Page not counted"
        )?;
    } else {
        writeln!(
            out,
            "# views: as recorded by the legacy '{}' pipeline at the time",
            h.source
        )?;
    }
    writeln!(
        out,
        "# columns: wiki = database name (e.g. enwiki); title = page title with underscores{}; \
         namespace = namespace number; views; files = Commons files used on the page, '|'-separated",
        if h.source == "dump" {
            ", namespace prefix included"
        } else {
            " (legacy data: usually without namespace prefix)"
        }
    )?;
    writeln!(out, "{COLUMNS}")?;

    let mut rows = 0;
    let mut buf = Vec::new();
    for site in sites {
        let giu = site.giu.clone();
        rows += reader.for_each_row(&giu, 0, |row| {
            buf.clear();
            buf.extend_from_slice(giu.as_bytes());
            buf.push(b'\t');
            row.write_tsv(&mut buf);
            out.write_all(&buf)?;
            Ok(())
        })? as u64;
    }
    out.flush()?;
    Ok(rows)
}

/// Legacy totals were counted differently from the rows that were kept;
/// say so rather than leave the reader to wonder.
fn rows_note(pages: u64, rows: u64) -> String {
    if pages == rows {
        String::new()
    } else {
        format!(" (as originally counted; {rows} rows below)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(dir: &Path, source: &str) -> PathBuf {
        let ym = YearMonth::new(2026, 9).unwrap();
        let path = gzb_path(dir, GroupId::new(3).unwrap(), &ym);
        let mut w = GzbWriter::new(&path, GroupId::new(3).unwrap(), &ym, source);
        let row = |title: &str, ns, views, files: &[&str]| GzbRow {
            title: title.to_string(),
            namespace_id: ns,
            views,
            files: files.iter().map(|s| s.to_string()).collect(),
        };
        w.add_site(
            "enwiki",
            vec![
                row("Bonn", 0, 30, &["B.jpg", "A.jpg"]),
                row("Köln", 0, 50, &[]),
            ],
            None,
        )
        .unwrap();
        w.add_site(
            "dewiki",
            vec![row("Kategorie:Rhein", 14, 7, &["C.jpg"])],
            None,
        )
        .unwrap();
        w.finish().unwrap();
        path
    }

    fn export_to_string(path: &Path, meta: &TsvMeta) -> Result<(u64, String)> {
        let mut out = vec![];
        let n = export(&mut GzbReader::open(path)?, meta, &mut out)?;
        Ok((n, String::from_utf8(out)?))
    }

    #[test]
    fn test_export_all() {
        let dir = std::env::temp_dir().join(format!("gzb_tsv_{}", std::process::id()));
        let path = sample(&dir, "dump");
        let meta = TsvMeta {
            group_label: Some("Category:Cologne (depth 5)".to_string()),
            wiki: None,
        };
        let (n, text) = export_to_string(&path, &meta).unwrap();
        assert_eq!(n, 3);
        let (comments, data): (Vec<&str>, Vec<&str>) =
            text.lines().partition(|l| l.starts_with('#'));
        assert!(comments.contains(&"# group: 3 — Category:Cologne (depth 5)"));
        assert!(comments.contains(&"# totals: 3 pages, 87 views on 2 wikis"));
        assert!(comments.iter().any(|c| c.contains("no bots")));
        // All comments come before the header.
        assert!(text
            .lines()
            .take(comments.len())
            .all(|l| l.starts_with('#')));
        assert_eq!(
            data,
            vec![
                COLUMNS,
                "enwiki\tKöln\t0\t50\t",
                "enwiki\tBonn\t0\t30\tA.jpg|B.jpg",
                "dewiki\tKategorie:Rhein\t14\t7\tC.jpg",
            ]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_export_one_wiki() {
        let dir = std::env::temp_dir().join(format!("gzb_tsv1_{}", std::process::id()));
        let path = sample(&dir, "sqlite3");
        let meta = TsvMeta {
            group_label: None,
            wiki: Some("dewiki".to_string()),
        };
        let (n, text) = export_to_string(&path, &meta).unwrap();
        assert_eq!(n, 1);
        assert!(text.contains("# group: 3\n"));
        assert!(text.contains("# this export: dewiki only, 1 pages, 7 views\n"));
        assert_eq!(rows_note(5, 5), "");
        assert_eq!(rows_note(4, 5), " (as originally counted; 5 rows below)");
        assert!(text.contains("legacy 'sqlite3' pipeline"));
        assert!(text.ends_with("dewiki\tKategorie:Rhein\t14\t7\tC.jpg\n"));

        let missing = TsvMeta {
            group_label: None,
            wiki: Some("frwiki".to_string()),
        };
        assert!(export_to_string(&path, &missing).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
