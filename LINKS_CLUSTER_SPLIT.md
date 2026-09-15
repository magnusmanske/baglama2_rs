# Commons links-cluster split (x4) — status and remaining work

Handoff note. Written 2026-09-15 while fixing
[glamtools issue #120](https://codeberg.org/magnusmanske/glamtools/issues/120).

## TL;DR

**Done as of 2026-09-15.** The Rust code was already fixed, pushed and built;
the missing `commons_links` deployment config entry has now been added on
Toolforge and the deployed checkout pulled. See
[What was done](#what-was-done).

One unrelated blocker remains and stops any run: the **tool DB (Trove) is
unreachable**. See [Open blocker](#open-blocker-tool-db-unreachable).

## The problem

Around **8 September 2026** WMF moved Commons' link tables off the main replica
section onto their own cluster (**x4**), exposed on the Wiki Replicas as a
separate host:

| | host | holds |
|---|---|---|
| core | `commonswiki.{web,analytics}.db.svc.wikimedia.cloud` | everything else: `file`, `filerevision`, `image`, `actor`, `user`, `revision`, … |
| links | `links.commonswiki.{web,analytics}.db.svc.wikimedia.cloud` | `categorylinks`, `collation`, `existencelinks`, `externallinks`, `globalimagelinks`, `imagelinks`, `iwlinks`, `langlinks`, `linktarget`, `pagelinks`, `templatelinks` |

Both hosts still serve a database called `commonswiki_p`, and both expose
`page` and `redirect` — those two are section-neutral.

The trap: **the old copies of the link tables are still present on the core
cluster, but frozen.** Querying them does not error. It returns data that was
correct up to the cutover and never changes again. That is how this went
unnoticed for a week — every tool reading `globalimagelinks` from the core
cluster simply reported that files added to articles after 8 September were
unused.

Measured on 2026-09-15, same query, both hosts:

```
                                  core (frozen)   links (live)
globalimagelinks rows, mkwiki         969,561        971,544
```

## What the code already does

Fixed in `32824c5`, pushed to `origin` (`efb54dd` is HEAD as of writing).

- `src/baglama2.rs` — two pools, `commons` (`POOL_COMMONS_CORE`) and
  `commons_links` (`POOL_COMMONS_LINKS`), both registered in `Baglama2::new()`.
- `Baglama2::get_commons_conn_for_tables(&["…"])` replaces "the Commons
  connection". It resolves the cluster from the table list via
  `DbCluster::for_tables` in the shared `wikimisc` crate, so the mapping lives
  in one place and survives another move.
- Every Commons call site passes its tables:
  `global_image_links.rs:33` (`globalimagelinks`), `baglama2.rs:555` and
  `baglama2.rs:597` (`page,categorylinks,linktarget` via
  `query_commons_repeat`), `baglama2.rs:610` (`image,actor,user` → core),
  `baglama2.rs:396` (`sites` → core).
- A query spanning both clusters is a hard **error**, not a silent wrong
  answer. Covered by `test_commons_pool_key_for_tables` in `src/baglama2.rs`.
- A missing `commons_links` config entry fails at startup with a message
  telling you what to add, rather than failing on the first links query.

`config.json.example` documents both pools already.

## What was done

The Toolforge build service builds from GitHub, so the **image was already
current** — build `glamtools-buildpacks-pipelinerun-m7v7p` ran
2026-09-14T14:05Z, one minute after the fix was pushed. Only the deployed
checkout and its gitignored `config.json` were stale.

Applied on Toolforge as `tools.glamtools` on 2026-09-15:

1. **Added the `commons_links` pool** to the deployed config, cloned from the
   existing `commons` entry (same credentials, same *analytics* endpoint) with
   the host rewritten to `links.commonswiki.analytics.db.svc.wikimedia.cloud`.

   Two copies of the config exist and both were patched, each backed up to
   `config.json.bak-20260915` first:

   - `/data/project/glamtools/baglama2_rs/config.json` — the one actually used.
     `Baglama2::new()` tries `config.json` relative to the cwd first and falls
     back to this absolute path; k8s jobs run with cwd `/workspace`, which has
     no `config.json`, so the fallback always wins.
   - `/data/project/glamtools/config.json` — was byte-identical to the
     pre-fix file. Unused at present, but it would win if anything ever ran
     from the tool home, so it was kept in sync rather than left as a trap.

2. **Pulled the deployed checkout**, `bec1aee` → `efb54dd`, fast-forward.

3. **Startup smoke test** (`toolforge jobs run … baglama2 _test`, one-off):
   `Baglama2::new` now gets past "adding tooldb + Commons core/links MySQL
   pools" — the config abort is gone. It then died on the tool DB, see below.
   Logs left in `~/linksfix-smoke.{out,err}`.

Re-running a month is still outstanding, blocked on the tool DB.

## Open blocker: tool DB unreachable

Independent of the cluster split, and it stops a run before any Commons query
happens. `tooldb` points at the Trove instance
`oxztsldqokc.svc.trove.eqiad1.wikimedia.cloud/baglama2`, which answers nothing:

- from a Toolforge k8s job: `No route to host (os error 113)`, 4/4 attempts
- through the bastion SSH tunnel: TCP connects, then
  `Lost connection to server at 'handshake: reading initial communication packet'`

So the port-forward path is fine and the instance itself is not serving —
check it in Horizon. This is the likely reason nothing has run since June, not
the startup abort (which had not been hit yet).

A pre-migration config for the old ToolsDB copy
(`tools.db.svc.wikimedia.cloud/s51203__baglama2_p`) is still on Toolforge as
`/data/project/glamtools/config.json.toolsdb`. Don't fail over to it blindly —
it predates the Trove migration and is presumably stale.

Also note `toolforge jobs list` is empty, so the monthly `rustbot` schedule
from `restart.sh` is not registered either; it needs re-creating once the tool
DB is back.

## Verifying it worked

Pick a file whose usage began after 8 September and compare three sources —
they should now agree, where before the tool disagreed with the other two:

```bash
# 1. ground truth (production DB, via the API)
curl -s 'https://commons.wikimedia.org/w/api.php?action=query&prop=globalusage&format=json&formatversion=2&titles=File:%D0%9A%D0%B0%D1%80%D0%BF%D0%B8%D0%BD%D1%81%D0%BA%D0%B8%20%D0%BC%D0%B0%D0%BD%D0%B0%D1%81%D1%82%D0%B8%D1%80%2010.jpg'

# 2. the live links cluster — should match (1)
mysql --defaults-file=~/replica.my.cnf \
  -h links.commonswiki.analytics.db.svc.wikimedia.cloud commonswiki_p \
  -e "SELECT gil_wiki,gil_page_title FROM globalimagelinks WHERE gil_to='Карпински_манастир_10.jpg'"

# 3. the frozen core copy — returns nothing; if the tool agrees with this, it is
#    still reading the wrong cluster
mysql --defaults-file=~/replica.my.cnf \
  -h commonswiki.analytics.db.svc.wikimedia.cloud commonswiki_p \
  -e "SELECT gil_wiki,gil_page_title FROM globalimagelinks WHERE gil_to='Карпински_манастир_10.jpg'"
```

`cargo test commons_pool_key_for_tables` checks the routing table without a DB.

Measured 2026-09-15 after the fix, `globalimagelinks` rows for `mkwiki`, same
query on both hosts, from Toolforge and again through the local tunnels in
`connect_db.sh` (3378 = links, 3377 = core):

```
links (live)    971,546 → 971,548 a few minutes later
core (frozen)   969,561 → 969,561, unchanged
```

## If you touch this code

- **Never add a join across the split.** `globalimagelinks` (links) joined to
  `image`/`file`/`actor` (core) cannot be served by one connection.
  `get_commons_conn_for_tables` returns `Err` for such a table list by design —
  resolve one side in code and pass it as an `IN (…)` list, which is what
  `get_files_from_user_name` → `GlobalImageLinks::load` already does.
- **Route by table list, never by a hard-coded pool key.** If you add a query,
  pass every table it reads. Getting this wrong does not error; it silently
  reads frozen data.
- `page` and `redirect` resolve to core when queried alone, because core is the
  first cluster that holds them. That is fine — they are identical on both.
- If Commons gets split again, add a pool key + a `DbCluster` arm in
  `Baglama2::commons_pool_key`, and update `DbCluster::for_tables` in
  [`wikimisc`](https://github.com/magnusmanske/wikimisc). Nothing else.

## The other half of this bug

The PHP tools in [`glamtools`](https://codeberg.org/magnusmanske/glamtools)
had the same fault and were fixed separately (GLAMorous 2's `globalusage.php`
+ `lib/GlobalUsage.php`, GLAMorous classic's `glamorous.php`, and the legacy
`baglama2/baglama.php`). They route through
`ToolforgeCommon::openDBwikiForTables()` / `dbForTables()` in the shared
`magnustools` lib, which is the PHP equivalent of `DbCluster::for_tables`.
Those changes are committed but **not yet pushed or deployed** as of
2026-09-15.
