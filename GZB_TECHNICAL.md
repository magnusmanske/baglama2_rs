# gzb — technical reference

Specification of the gzb view-data format and of the files and database
state around it. For running and deploying, see
[GZB.md](GZB.md).

- [1. Overview](#1-overview)
- [2. File layout](#2-file-layout)
- [3. Header](#3-header)
- [4. Data area and rows](#4-data-area-and-rows)
- [5. Invariants](#5-invariants)
- [6. Reading](#6-reading)
- [7. Writing](#7-writing)
- [8. Location and database integration](#8-location-and-database-integration)
- [9. Data provenance](#9-data-provenance)
- [10. Pipeline-internal files](#10-pipeline-internal-files)
- [11. Versioning](#11-versioning)
- [12. Implementations](#12-implementations)

## 1. Overview

A gzb file holds the view data of one **group** (a tracked Commons category
tree, or a user's uploads) for one **month**: every wiki page that uses one
of the group's files, with its monthly views and the files it uses.

It is designed around the two questions the web API asks:

| API action | Needs | gzb answers it with |
|---|---|---|
| `month_overview` | per-wiki page and view totals | the header alone; nothing is decompressed |
| `month_site` | one wiki's pages, most-viewed first, usually the top 100 | one wiki's first chunk(s) |

Hence: a JSON header with per-wiki totals and chunk offsets, and per-wiki
rows, sorted by views, compressed in independent chunks. Plain gzip keeps
the format readable with PHP's built-in `gzdecode` and with standard tools.

Measured sizes: a full month of all 1,280 groups is 530 MB (2026-09; the
SQLite files for 2022-01, 948 groups, took 16 GB). The largest group (979,
7.3M pages) is 41 MB; a 2017 group-month with 20,908 legacy rows is 670 KB.

## 2. File layout

```
+-------------------------------------------+
| BAGLAMA-GZB <version> <header_len>\n      |  first line, ASCII
+-------------------------------------------+
| <header_len bytes: JSON header, UTF-8>    |  section 3
+-------------------------------------------+  ← data start
| gzip member                               |
| gzip member                               |  section 4
| …                                         |
+-------------------------------------------+
```

**First line.** Three fields separated by single spaces, ending in `\n`:

| Field | Value |
|---|---|
| magic | `BAGLAMA-GZB` |
| version | `1` (decimal) |
| header_len | length of the JSON header in **bytes** (decimal, > 0) |

Example: `BAGLAMA-GZB 1 4711\n`.

**Data start** is `len(first line including \n) + header_len`. All chunk
offsets are relative to it.

There is no trailer, checksum or padding; the file ends with the last chunk.

## 3. Header

UTF-8 JSON, written compactly (no whitespace). Example (pretty-printed):

```json
{
  "version": 1,
  "group_id": 979,
  "year": 2026,
  "month": 9,
  "source": "dump",
  "created": "2026-10-07T11:51:28Z",
  "total_views": 41783396,
  "total_pages": 7279332,
  "sites": [
    {
      "giu": "enwikisource",
      "pages": 7100211,
      "views": 40213377,
      "chunks": [
        { "offset": 0,      "length": 81234, "rows": 5000 },
        { "offset": 81234,  "length": 79870, "rows": 5000 },
        { "offset": 161104, "length": 12455, "rows": 211 }
      ]
    },
    { "giu": "dewiki", "pages": 0, "views": 0, "chunks": [] }
  ]
}
```

### Top level

| Field | Type | Meaning |
|---|---|---|
| `version` | integer | Format version; equals the version on the first line. |
| `group_id` | integer | `groups.id` in the tool DB. |
| `year`, `month` | integer | The month the views are for (`month` 1–12). |
| `source` | string | `dump`: generated from the pageview dump by `gzb_month`. `sqlite3`, `mysql`, `file`: converted from that legacy storage by `gzb_convert`. |
| `created` | string | When the file was written; RFC 3339, UTC, whole seconds. |
| `total_views` | integer | Sum of `sites[].views`. |
| `total_pages` | integer | Sum of `sites[].pages`. |
| `sites` | array | One entry per wiki, sorted by `views` descending, then `giu` ascending. |

### `sites[]`

| Field | Type | Meaning |
|---|---|---|
| `giu` | string | Wiki database name (`enwiki`, `commonswiki`, `wikidatawiki`); matches `sites.giu_code` in the tool DB. Unique within the file. |
| `pages` | integer | Page count shown in the month overview. |
| `views` | integer | View total shown in the month overview. |
| `chunks` | array | This wiki's rows, in order. Empty if it has none (possible for converted data, see 9.2). |

### `sites[].chunks[]`

| Field | Type | Meaning |
|---|---|---|
| `offset` | integer | Byte offset of the gzip member, relative to data start. |
| `length` | integer | Byte length of the gzip member. |
| `rows` | integer | Number of rows in it (1–5000). |

All integers are non-negative and fit in 64 bits.

## 4. Data area and rows

The data area is a sequence of **gzip members** (RFC 1952), one per chunk,
each complete and independently decompressible. Each decompresses to UTF-8
text: one row per line, every line terminated by `\n`.

### Row format

```
title \t namespace_id \t views \t files \n
```

| Column | Content |
|---|---|
| `title` | Page title, underscores for spaces. See 9.3 for namespace prefixes. |
| `namespace_id` | Namespace number (signed decimal integer). |
| `views` | Monthly views (decimal integer). |
| `files` | Commons file names used on this page, joined by `\|`; empty if none are recorded. File names have underscores and no `File:` prefix. |

A row with no files ends in `\t\n`. There is no quoting or escaping: titles
and file names cannot legally contain tabs or newlines, and file names
cannot contain `|`. As a safeguard, writers replace any `\t`, `\n` or `\r`
in a title or file name with a space, and any `|` in a file name with `_`.

Example chunk content:

```
Rome	0	700	A.jpg|B.jpg
Kategorie:Rhein	14	7	C.jpg
Italy	0	5	
```

## 5. Invariants

Writers guarantee, and readers may rely on:

1. Within a wiki, rows across its chunks in `chunks` order are sorted by
   `views` descending, then `title` ascending (byte order). The first rows
   of the first chunk are the wiki's most-viewed pages.
2. Every chunk has exactly `rows` rows; all chunks but a wiki's last have
   5000.
3. Within a wiki, each `title` occurs once — in generated files
   (`source: dump`). Converted files keep the legacy rows, which were keyed
   by page ID and may in rare cases repeat a title.
4. Within a row, `files` is sorted ascending (byte order) and has no
   duplicates.
5. `sites` is sorted by `views` descending, then `giu` ascending.
6. `total_views` and `total_pages` are the sums over `sites`.
7. Chunks never overlap, and together they cover the data area exactly.

Readers must **not** rely on:

- **The order of chunks in the data area.** It is the order the wikis were
  written in, not the header order. Always seek by `offset`.
- **`pages` matching the row count.** It does for generated files
  (`source: dump`). For converted files it is the legacy stored total,
  which the old pipelines counted differently (see 9.2). The row count is
  the sum of `chunks[].rows`.
- **`views` matching the sum of row views** in converted files, for the
  same reason.
- **The gzip header fields** (mtime, OS, file name); writers leave them at
  library defaults.

## 6. Reading

Opening a file:

1. Read the first line. Check that it has three space-separated fields,
   that the magic is `BAGLAMA-GZB` and that the version is supported
   (section 11). Parse `header_len`.
2. Read exactly `header_len` bytes and parse them as JSON.
3. Data start = byte length of the first line, including `\n`, plus
   `header_len`.

The top `max` rows of wiki `giu` (all rows if `max` is 0):

```
site = header.sites.find(s => s.giu == giu)       # none → no rows
out = []
for chunk in site.chunks:                         # in array order
    seek(data_start + chunk.offset)
    text = gunzip(read(chunk.length))
    for line in text.split_lines():
        out.append(parse_row(line))               # split on \t, at most 4 fields
        if max > 0 and len(out) == max: return out
return out
```

Split each line on the first three tabs only, and split `files` on `|`
only when it is non-empty.

Readers must treat malformed input (bad magic, unsupported version, a
header that is not JSON or lacks `sites`, a chunk that fails to
decompress) as an error.

### With standard tools

Adjacent gzip members form a valid multi-member gzip stream, so:

```bash
f=979.gzb
line=$(head -1 "$f" | wc -c)                  # first line, bytes incl. \n
len=$(head -1 "$f" | cut -d' ' -f3)           # header_len
tail -c +$((line + 1)) "$f" | head -c "$len"  # the JSON header
tail -c +$((line + len + 1)) "$f" | zcat      # every row of every wiki
```

The second command gives the rows in data-area order; which wiki a row
belongs to is only known from the header offsets. `baglama2 gzb_show` and
`baglama2 gzb_tsv` do this properly.

## 7. Writing

The reference writer (`GzbWriter`, `src/gzb/mod.rs`):

1. Builds each wiki's rows, sorted per invariant 1, and compresses them in
   chunks of 5000 rows (gzip level 9) into an in-memory data area. The
   chunks' offsets and lengths go into the wiki's header entry.
2. Sorts `sites` per invariant 5 and serializes the header.
3. Writes the first line, header and data area to `<name>.gzb.tmp` next to
   the target, `fsync`s it, then renames it over `<name>.gzb`. Readers
   therefore see either the old file or the complete new one, never a
   partial one.

The data area of a whole group is held in memory while writing; it is the
compressed size, so ~41 MB for the largest group.

`GzbWriter::add_site` takes the rows as `GzbRow` values and sorts them.
`add_site_sorted` takes a row count and a callback that writes row *i*;
`gzb_month` uses it with compact page records, so it never needs one
`GzbRow` per page.

## 8. Location and database integration

### Path

```
<gzb root>/<YYYYMM>/<group_id>.gzb
```

`<gzb root>` is `gzb_data_root_path` from `config.json` (required). On
Toolforge that is
`/data/project/glamtools/viewdata/gzb`. `YYYYMM` is zero-padded, e.g.
`202609`.

The path is not stored in the database. Both the Rust code (`gzb_path`) and
PHP (`Baglama::constructGzbFilename`) build it from group ID and month, so
the root must agree between them.

### `group_status`

One row per group and month (unique on `group_id, year, month`):

| Column | For gzb |
|---|---|
| `storage` | `'gzb'`, the only value since all legacy storage was converted. |
| `status` | `GENERATING PAGE LIST` → `SCANNED` → `VIEW DATA COMPLETE`, or `FAILED`. Only `VIEW DATA COMPLETE` rows are listed by the API. |
| `total_views` | Header `total_views`, clamped to 2,147,483,647 (the column is a signed `INT`). Set on completion. Converted rows kept their legacy value. |

### API mapping (`Baglama2Api.php`)

| Action | Reads |
|---|---|
| `month_overview` | Header `sites[]`; `server`, `name`, `language` and `project` are joined from the tool DB `sites` table on `giu_code`. |
| `month_site` | `rows(giu, max)`; returns `title`, `namespace_id`, `views` and `files` per row. |

## 9. Data provenance

### 9.1 Generated files (`source: dump`)

Written by `gzb_month` (`src/gzb/month.rs`):

- **Pages:** the group's files come from the Commons category tree (to the
  group's depth, namespace 6, no redirects) or the user's uploads. Their
  usage comes from `globalimagelinks` on the Commons links replica.
- **Views:** the monthly `-user` pageview dump
  (`pageview_complete/monthly/YYYY/YYYY-MM/pageviews-YYYYMM-user.bz2`):
  user agents (no bots or spiders), all platforms, summed over access
  types. Pages are matched by `(dump wiki code, title)`; the dump code is
  the wiki's host name without `www.` and `.org` (`enwiki` → `en.wikipedia`,
  `wikidatawiki` → `wikidata`). Pages titled `Main_Page` get 0 views, as in
  the legacy pipeline.
- **Totals:** `pages` is the row count and `views` the sum of row views.
- Pages with 0 views are kept as rows.

### 9.2 Converted files (`source: sqlite3`, `mysql`, `file`)

Written by `gzb_convert` (`src/gzb/convert.rs`, removed once every legacy
group-month was converted, October 2026). Rows are what the legacy API
showed; totals are what the legacy month overview showed. The sources are
deleted, so these files are the only copy.

| Source | Rows from | `pages` / `views` from | Not carried over |
|---|---|---|---|
| `sqlite3` | `views` with `done=1`, joined to `group2view` for files | `gs2site` (first row per wiki); computed if missing | the `files` table (full category file list), `done≠1` rows, wiki page IDs |
| `mysql` | tool DB `group2view` → `views` → `pages`, `files` | `gs2site` (first of duplicate rows) | page IDs; titles repaired if double-encoded UTF-8 |
| `file` | the flat file (`giu`, URL-encoded title, files, views) | computed | — |

Wikis that appear in `gs2site` but have no rows get a header entry with
empty `chunks`, so the overview is unchanged. Rows on a wiki whose legacy
site ID cannot be mapped to a `giu` code are dropped, with a warning in the
log.

### 9.3 Titles

- Generated files: the title as in the pageview dump, i.e. with the
  wiki's **local** namespace prefix (`Kategorie:Köln` on dewiki).
- Converted files: as the legacy store had it, usually **without** a
  namespace prefix; `namespace_id` tells the namespace.

## 10. Pipeline-internal files

These exist only while `gzb_month` runs, in
`<gzb root>/work/<YYYYMM>/`, and are removed after a fully successful run
over all groups. They are documented for debugging, not as an interface.

### Page lists: `<group_id>.tsv.gz`

gzip (level 1) of lines `giu \t namespace_id \t dump_title \t file`, one per
use of a file on a page, in no particular order. Written to `.tmp` and
renamed when complete; its presence means phase 1 is done for the group.

### View table: `views.bin`

| Offset | Size | Content |
|---|---|---|
| 0 | 10 | `BGZVIEWS1\n` |
| 10 | 8 | `n`, entry count, u64 little-endian |
| 18 | 12·n | entries: page key (u64 LE), views (u32 LE) |

Entries are sorted by key, and only pages with views > 0 are stored.
`views.bin` is reused if it is newer than every page list in the run.

The **page key** is FNV-1a 64 over `dump_code`, a `0x00` byte and the
title, followed by the splitmix64 finalizer. It must not change between
builds, because `views.bin` persists across runs. A test pins
`page_key("en.wikipedia", "Foo") = 0xa807bd7bd6b606e6`.

Keys are 64-bit hashes, so collisions are possible. Their only effect is
that one page receives another page's views. At ~25M pages and ~440M dump
lines a month:

| Collision | Chance per month |
|---|---|
| two of the group pages share a key | about 1 in 60,000 |
| some unrelated dump title has a page's key | about 1 in 1,700 |

In memory, the keys form a `ViewTable`: sorted keys, parallel view counts,
and an index of bucket starts on the top key bits, about four keys per
bucket. That is 12 bytes per page, ~300 MB for 25M pages.

### Conversion log: `<gzb root>/conversion.log`

Written by `gzb_convert` for each switched row; tab-separated:

```
time  group_status.id  group_id  YYYYMM  old storage  source
```

A record of where every converted group-month came from. It was the
rollback path while the sources existed; they are deleted now.

## 11. Versioning

The version on the first line and in the header is `1`. Readers reject any
other version.

- **Adding header fields** does not change the version. Both reference
  readers ignore unknown JSON fields.
- **Changing** the first line, the meaning or type of an existing field,
  the row format or the chunk encoding requires a new version. Readers
  must support it before writers produce it.

## 12. Implementations

| | Language | Where | API |
|---|---|---|---|
| Writer | Rust | `src/gzb/mod.rs` | `GzbWriter::new`, `add_site`, `add_site_sorted`, `finish` |
| Reader | Rust | `src/gzb/mod.rs` | `GzbReader::open`, `header`, `rows`, `for_each_row` |
| Reader | PHP | glamtools `public_html/lib/GzbReader.php` | `new GzbReader($file)`, `sites()`, `rows($giu, $max)` |
| TSV export | Rust | `src/gzb/tsv.rs` | `export`; command `gzb_tsv` |

Commands that work with the files directly:

```bash
baglama2 gzb_show GROUP YEAR MONTH            # header: per-wiki totals
baglama2 gzb_show GROUP YEAR MONTH enwiki     # top pages of one wiki
baglama2 gzb_tsv  GROUP YEAR MONTH [WIKI] [--out=FILE]
```

Tests: `cargo test gzb` (format round trip, chunking, sorting, conversion,
TSV export, view table) and `php tests/run.php` in glamtools (PHP reader
against a file built from this specification).
