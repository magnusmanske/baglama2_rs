# gzb view data — one compressed file per group-month

Written 2026-10-07. Replaces the `viewdata_*` MySQL tables (tool DB out of
space) and the per-group SQLite files (462 GB on NFS), and drops the
per-article pageview API entirely.

## Why it works

The web API (`glamtools/public_html/baglama2/Baglama2Api.php`) only asks two
things of a group-month: per-wiki totals, and one wiki's pages sorted by
views. Both are precomputable, so the data is a static file, not a database:

```
viewdata/gzb/<YYYYMM>/<group_id>.gzb
```

`BAGLAMA-GZB 1 <len>\n` + a JSON header (per-wiki totals + chunk offsets) +
gzip members of ≤5000 rows each (`title \t ns \t views \t files`), sorted by
views. "Top 100 of enwiki" inflates one chunk. PHP reads it with plain
`gzdecode` (`GlamTools\GzbReader`). Format details: `src/gzb/mod.rs`.

Measured on a real 2017 group-month: 20,908 legacy rows → 670 KB.

## Generating a month (`gzb_month`)

```bash
./run_gzb.sh month 2026 9      # checks first, then generates
./run_gzb.sh check 2026 9      # just the check, changes nothing
```

`gzb_month` runs the same preflight as `gzb_check` (dump, replicas, tool DB,
output dirs) every time it starts, re-runs included, and stops before
touching anything if a problem is found. `--no-check` skips it; a missing
dump then fails at phase 2, and a re-run resumes from the saved page lists.

Three phases (`src/gzb/month.rs`), each resumable — after a crash, re-run
the same command:

1. **Page lists** (replicas): category tree / uploader → files →
   `globalimagelinks` → `viewdata/gzb/work/<YYYYMM>/<gid>.tsv.gz`.
   Status `GENERATING PAGE LIST` → `SCANNED`.
2. **Views**: every page becomes a 64-bit hash key in one in-memory table
   (~25M pages ≈ 600 MB); one pass over
   `/public/dumps/public/other/pageview_complete/monthly/YYYY/YYYY-MM/pageviews-YYYYMM-user.bz2`
   fills it. Saved as `work/<YYYYMM>/views.bin`, reused if newer than all
   page lists.
3. **Files**: page list + views → `.gzb`. Status `VIEW DATA COMPLETE`,
   storage `gzb`, `total_views` set.

The work dir is removed after a fully successful run over all groups.
`--groups=1,2` limits a run (and keeps the work dir); `--force` regenerates
groups that are already complete — including legacy ones, so careful.
Groups with complete legacy data are skipped by default; `mysql2` rows are
not (they never got view counts).

Matching is by `(dump wiki code, namespace-prefixed title)`, using
`gil_page_namespace` — same form as the dump, no API calls for namespace
names. Dump codes come from the site matrix (host minus `www.` and
`.org`: `wikidatawiki` → `wikidata`), falling back to `sites.server` for
closed wikis, which the site matrix omits but the dump still has. Checked
against every code in the 2026-09 dump: all wikis with views map, except
the closed `strategy.wikimedia` and `usability.wikimedia` (tool DB has them
as `.wikipedia.org`; ~90k views/month combined). Private and deleted wikis
have no dump entries anyway. Main_Page views are ignored, as before.

## Converting legacy data (`gzb_convert`)

```bash
./run_gzb.sh convert --dry-run                       # counts, missing sources, sizes
./run_gzb.sh convert --storage=file                  # 2010–2014 flat files
./run_gzb.sh convert --storage=mysql --from=201402 --to=201412
./run_gzb.sh convert --storage=sqlite3 --limit=100 --no-switch   # trial, no DB change
```

Each converted file is read back before `group_status.storage` is switched to
`gzb`; per-wiki totals are taken from the legacy `gs2site` so the overview
does not change. **Sources are never modified or deleted.** Every switch is
logged to `viewdata/gzb/conversion.log` (time, group_status id, group,
YYYYMM, old storage, source); switching back is
`UPDATE group_status SET storage='<old>' WHERE id=<id>`.

Freeing the space is a separate, manual step once you are happy:
`viewdata/<YYYYMM>/*.sqlite3` per converted month, and for the `mysql` era
the tool DB tables `group2view`, `views`, `gs2site` (34.6 GB) once
`SELECT COUNT(*) FROM group_status WHERE storage='mysql'` is 0. `pages`,
`files` and the `viewdata_*` tables are only used by the `mysql2` pipeline.

`mysql2` months (2026-01, 2026-05; 2025-12 was dropped) are not convertible
— no views. Regenerate them with `gzb_month 2026 1` etc.; page lists then
reflect current category contents.

## Deploying

1. PHP first (`glamtools`): `GzbReader`, the `gzb` branches in
   `Baglama2Api.php`, `Baglama::constructGzbFilename`. Without it, a `gzb`
   group-month shows "Unknown storage type".
2. Push this repo, then on Toolforge as `tools.glamtools`:
   `cd ~/baglama2_rs && git pull && ./build.sh` (builds the image from
   GitHub; `toolforge build show` for progress).
3. `./run_gzb.sh check 2026 9`, then `./run_gzb.sh month 2026 9`.
4. Monthly: `./run_gzb.sh schedule` (3rd of the month, last month).

The first run adds `'gzb'` to the `group_status.storage` enum (metadata-only
ALTER).

Job limits: 6 GiB / 3 CPU per job, 8 GiB for the tool; jobs ask for 5 GiB.
