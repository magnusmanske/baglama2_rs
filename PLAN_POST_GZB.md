# Plan: after the gzb migration

Written 2026-10-08.

State at writing: all `file` group-months have been converted to `gzb`.
`sqlite3` is in progress and `mysql` will follow. New months are generated
with `gzb_month`. See [GZB.md](GZB.md) for running the pipeline and
[GZB_TECHNICAL.md](GZB_TECHNICAL.md) for the format.

Order of work: 1 is done (2026-10-08). 2 waits until conversion is complete
and verified. The rest can go in whenever convenient.

## 1. Retire the legacy generation pipelines

`gzb_month` replaces every path that produced view data. The old ones are
roughly half the crate.

Remove:
- [x] Commands in `src/main.rs`: `mysql2`, `mysql2_views`, `_run`, `_next`,
      `_next_all`, `_next_all_seq`, `_backfill`, `_test`, plus
      `process_mysql2`, `process_mysql2_views` and `process_all_groups`.
- [x] `src/db_trait.rs`, `src/db_sqlite.rs`, `src/db_mysql2.rs` (about 2,000
      lines).
- [x] `src/group_date.rs` (all of it), `src/month_views.rs`,
      `src/view_count.rs` and `src/pageviews/api_fallback.rs`. Also
      `src/file.rs`, `src/page.rs`, `src/row_group_status.rs`, the legacy
      parts of `dump_reader.rs` and `Baglama2`, and seven dependencies.
      Modules are now private, so rustc reports dead code.
- [x] Old launch scripts: `run_views.sh`, `run_single.sh`,
      `run_single_wait.sh`, `restart.sh`.

Keep: `src/gzb/`, `src/pageviews/dump_reader.rs`,
`src/global_image_links.rs`, and whatever `Baglama2` needs for them.

Before removing:
- [x] Move `DbMySql2::repair_double_encoding` into `src/gzb/`. It is a pure
      function and gzb's only dependency on `db_mysql2.rs` (used by
      `repair_title` in `convert.rs`).
- [x] **Check Toolforge for a stale `rustbot` job** (`toolforge jobs list`).
      `restart.sh` schedules `baglama2 next_all lm lm` for the 2nd of each
      month, but `next_all` no longer exists (only `_next_all`). If the job
      is still scheduled, it runs `deactivate_nonexistent_categories` and
      then panics every month. Checked 2026-10-08: not scheduled; only
      `gzb-monthly` exists.

## 2. End of `gzb_convert`

While legacy sources exist, `conversion.log` plus the sources is the
rollback path, so keep the conversion code until then.

When `SELECT storage,COUNT(*) FROM group_status GROUP BY storage` shows no
`file`, `sqlite3` or `mysql` rows:

- [ ] Regenerate the `mysql2` months (2026-01, 2026-05) with `gzb_month`, so
      no `mysql2` rows remain either.
- [ ] **Add and run `gzb_verify` before deleting any source.** For every gzb
      file: it reads back, every chunk decompresses (gzip already has a CRC32
      per member), the header totals match `group_status.total_views`, and the
      chunk row counts are consistent. Once the sources are gone, a bad file
      cannot be regenerated.
- [ ] Set up an off-NFS backup of `viewdata/gzb/` (see 3) before deleting.
- [ ] Delete the legacy sources: `viewdata/<YYYYMM>/*.sqlite3`, the flat files
      named in `group_status.file`, and the tool DB tables `group2view`,
      `views`, `gs2site` (34.6 GB), `pages`, `files` and `viewdata_*`.
- [ ] Drop the `group_status.file` and `group_status.sqlite3` columns, and
      narrow the `storage` enum. That leaves the tool DB with roughly
      `groups`, `group_status` and `sites`.
- [ ] Remove `src/gzb/convert.rs`, the `gzb_convert` command, the
      `run_gzb.sh convert` case, the `rusqlite` dependency (its bundled SQLite
      build is a large share of compile time), `baglama.sqlite3_schema`, and
      `sqlite_data_root_path` in `Baglama2`. Careful: without a
      `gzb_data_root_path` key in `config.json`, the gzb root is
      `<sqlite_data_root_path>/gzb`, so set that key first.
- [ ] PHP (`glamtools`): remove the `file`, `sqlite3`, `mysql` and `mysql2`
      branches from `Baglama2Api.php`, so gzb is the only storage and the
      "Unknown storage type" failure mode goes away.
- [ ] Update `GZB.md` (the conversion section) and the memory notes.

## 3. gzb files are the only copy

After step 2, `viewdata/gzb/` on NFS holds all view data, at about
530 MB a month.

- [ ] Back up somewhere other than NFS, and do it before deleting sources.
- [ ] Optional: a header checksum in a future format version. Chunk data is
      already covered by gzip's CRC; the JSON header is not. Only worth it
      together with another reason to bump `VERSION`, as PHP has to follow.

## 4. Read-only commands without the tool DB

`gzb_show` and `gzb_tsv` go through `Baglama2::new()`, which connects to the
tool DB (Trove), loads sites and calls the Wikidata API. When Trove was down
(2026-09-15), even reading a local gzb file failed.

- [ ] Dispatch `gzb_show`/`gzb_tsv` before `Baglama2::new()`. They need only
      the gzb root (from `config.json`) and the file. Make the group label in
      the TSV comments best-effort: try the DB with a short timeout and leave
      the label out if that fails.

## 5. Smaller cleanups, once 1 is done

- [ ] Replace the hand-rolled argument parsing in `main.rs` (`positional`,
      `VALUE_FLAGS`, `flag_value`, `parsed_flag`, `has_flag`,
      `group_ids_flag`) with `clap`. That is a quick change with only the
      `gzb_*` commands left, and gives correct per-command `--help`.
- [ ] Split `Baglama2` (config, DB pools, sites cache, category queries,
      group status) into config/connections plus a small `group_status`
      module.
- [ ] Benchmark `gzb_month` and `gzb_convert` without mimalloc's `secure`
      feature. It costs speed and memory for hardening this batch tool does
      not need.

## Not changing

- The gzb format itself: it works. At most, the header checksum in 3.
- Converted data does not keep the full category file list, `done!=1` rows,
  or wiki page IDs. That was decided on 2026-10-07; don't reopen it.
