# Airflow Metadata DB — Maintenance & Cleanup

Single reference for the two system DAGs that keep the Airflow metadata Postgres
healthy, plus the one-time autovacuum tuning. Everything here is also documented
in each DAG's docstring; this file is the consolidated picture.

---

## TL;DR

| Concern | Handled by | Automated? |
|---|---|---|
| Row growth / retention | `system_airflow_db_cleanup_v2` (weekly Sat) | ✅ deletes old rows |
| Dead tuples (VACUUM) | autovacuum (tuned) | ✅ continuous |
| **Index bloat** | `system_airflow_db_maintenance` → `REINDEX CONCURRENTLY` | ✅ **online, gated** |
| **XID/MultiXact wraparound** | `system_airflow_db_maintenance` → `VACUUM (FREEZE)` | ✅ **online, gated** |
| **Stale planner stats** | `system_airflow_db_maintenance` → `ANALYZE` (+ autoanalyze) | ✅ **online, gated** |
| Long/idle-txn blockers | maintenance DAG daily email | 🔔 **alert only** (kill is destructive; clear manually) |
| Heap bloat / disk reclaim (`VACUUM FULL`) | one-time, manual, in a window | ❌ not automated (downtime; disk only) |

**Three online corrective actions — REINDEX, FREEZE, ANALYZE — all no-downtime,
each gated by its own flag + threshold and each auto-detecting the tables it acts
on.** The only thing that genuinely needs downtime is `VACUUM FULL` (and it's never
needed for a *performance* problem — the three online actions cover those).

---

## Automation scope (important)

The maintenance DAG runs **three online corrective actions**, each gated by its own
flag **and** threshold, each auto-detecting the tables it touches (no hardcoded names):

| Action | SQL | Fires when | Gate flag / threshold |
|---|---|---|---|
| **Reindex** | `REINDEX TABLE CONCURRENTLY` | est. index bloat ≥ threshold | `db_maintenance_enable_reindex` + `…_reindex_bloat_pct` (30) |
| **Freeze** | `VACUUM (FREEZE)` | relfrozenxid/relminmxid age ≥ threshold | `db_maintenance_enable_freeze` + `…_wraparound_warn_pct` (60) |
| **Analyze** | `ANALYZE` | unanalyzed changes ≥ threshold | `db_maintenance_enable_analyze` + `…_stale_mods` (1,000,000) |

All three are **online (no downtime)**. **FREEZE + ANALYZE default ON** (safe, protective,
run unattended — just set the alert email); **REINDEX defaults OFF** (opt-in, since the
first run on a long-unmaintained DB can rebuild very large indexes — enable it per env
after a glance at the report). **Only REINDEX skips on a sustained (> `db_maintenance_block_txn_min`) blocking
transaction** — `REINDEX CONCURRENTLY` would *wait* on it, and since this DAG runs one
action at a time, a multi-hour wait would stall freeze/analyze/monitoring too, so it
skips + retries next run (flagged in the email). **FREEZE and ANALYZE always run when
enabled** — neither waits on a transaction, so gating them would only forfeit progress
(a `VACUUM (FREEZE)` freezes everything older than the oldest open txn and returns).

**Still alert-only** (not auto-acted): **long/idle-txn blockers** — `pg_terminate_backend`
is destructive, so a human clears them. Dead-tuple lag is handled continuously by autovacuum.

**Not automatable without downtime** (the one exception): **`VACUUM FULL`** — takes an
`ACCESS EXCLUSIVE` lock (downtime) and only returns disk to the OS (irrelevant on RDS).
Done once, in a planned window, by support. The three online actions cover every
*performance* problem; `VACUUM FULL` is never needed for that.

---

## DAG 1 — `system_airflow_db_maintenance` (DAILY, 03:00 UTC)

Proactively monitors DB health A–Z and reindexes bloated indexes online. Sends **one consolidated email every run**.

### Flow
`snapshot_before → reindex_if_bloated → freeze_if_old → analyze_if_stale → build_report → email_report`
(one online action at a time; + `email_failure` fires only if a task actually errors)

### What it monitors (read-only, every run)
- index bloat % per table
- dead tuples + autovacuum age
- planner-stats staleness (`n_mod_since_analyze`, `last_autoanalyze`)
- **XID + MultiXact wraparound age** — as **% of the ~2.1B read-only limit** (healthy is single digits; a DB merely riding the autovacuum force-freeze trigger sits ~9%, *not* near danger)
- **long / idle-in-transaction sessions** (root cause of the original bloat)

### The email (always sent, one per run)
**Subject:** `[DB Maintenance][<region-env>] <STATUS> - <date>`
where `<region-env>` = `REGION`-`AIRFLOW_ENVIRONMENT` (falls back to `unknown`), and
`<STATUS>` = `ATTENTION` / `WARNING` / `HEALTHY` (or `FAILED` for the failure email).
Filter on it: **ATTENTION = act, WARNING = skim, HEALTHY = ignore.**

**Severity model** — the level answers *"does a human need to act?"*:
- 🔴 **ATTENTION** — action required (an xmin pin holding wraparound; leftover `_ccnew` indexes; an enabled action that failed; a blocked action).
- 🟠 **WARNING** — review only, self-healing or a config choice: wraparound elevated **but nothing pinning xmin** (autovacuum will fix it), bloat/stale while that action is disabled (report-only), or a long transaction open. *"Check with your DBA if it persists."*
- 🟢 **HEALTHY** — nothing.

**Body:**
1. **Status banner** — 🔴 **ATTENTION NEEDED** (action-required list), 🟠 **WARNING** (for-review list), or 🟢 **HEALTHY**. Action-required and for-review items are shown as separate lists.
2. **Action lines** — one each for **Reindex / Freeze / Analyze**: what it did (X tables + before/after where relevant), or report-only / blocked / nothing-to-do.
3. **Wraparound table** — XID and MultiXact age as **% of the ~2.1B read-only limit** (single digits = healthy), for the **connected Airflow DB only** (wraparound is a cluster-wide counter, but the static system DBs are never the trigger; this churn DB is always the oldest). *Note: this is the real danger horizon, not `autovacuum_freeze_max_age` — a DB riding the normal force-freeze trigger is ~9%, not "98%".*
4. **Long/idle-in-transaction sessions** — pid, user, state, age, query (or "none").
5. **Leftover / invalid indexes** — `_ccnew` / `_ccold` (or otherwise invalid) indexes left behind by an interrupted `REINDEX CONCURRENTLY`, with size, kind, and the suggested **manual** `DROP INDEX CONCURRENTLY` action (or "none"). **The DAG never drops anything** — this is report-only so a human clears them (`_ccnew` = rebuild never finished, drop then reindex again; `_ccold` = rebuild succeeded, just drop the old one).
6. **Per-table health table** — `rows, dead%, heap, indexes, total, est_bloat% (before→after), mods_since_analyze, hrs_since_vac, hrs_since_analyze, reindexed (idx before→after)`.
   - **Row colour is tied to the banner** (never contradicts it): 🟩 green = reindexed/handled this run; 🟥 red = over the bloat threshold and *not* handled (matches an ATTENTION line); plain = fine. *(The only red in a HEALTHY email is the legend swatch that defines the colour — no data row is red when nothing needs attention.)*
   - `est_bloat%` shows **before→after** the run's reindex, **only for indexes ≥ the size floor** (`db_maintenance_reindex_min_mb`, 100 MB); smaller indexes show `—` because the estimate is noise at that size. Note a near-minimum index sits at its structural floor, so its estimate stays high and **reindex cannot lower it** — that's expected, not a failure.

**ATTENTION is raised if any of:** a table over the bloat threshold not reindexed;
wraparound ≥ warn % (still, after any freeze); a long/idle txn open; stale planner
stats not analyzed; a reindex/freeze blocked by a transaction.

### Reindex behaviour (the automated action)
- **Per-TABLE** `REINDEX TABLE CONCURRENTLY` (not per-index — deliberate: our churn
  tables update indexed columns so all their indexes bloat together).
- **Online / no downtime.** `lock_timeout` set; `statement_timeout=0`.
- **`retries=0`, `execution_timeout=7 days`** — never auto-retry (re-runs hours of
  work, can stack `_ccnew` leftovers) and never get killed mid-flight.
- **Skipped if a long/idle txn is open** (would stall) → flagged in the email; no auto-cleaner, clear it manually.
- **Leftover `_ccnew`/`_ccold` indexes are detected and listed in the email** (read-only), but **never dropped by the DAG** — clear them manually with `DROP INDEX CONCURRENTLY`. Suffix meaning and recovery per the [PostgreSQL REINDEX docs — "Rebuilding Indexes Concurrently"](https://www.postgresql.org/docs/current/sql-reindex.html) (`_ccnew` = rebuild never finished, drop then reindex again; `_ccold` = rebuild succeeded, just drop the old one).

### Variables
| Variable | Default | Meaning |
|---|---|---|
| `db_maintenance_alert_email` | **(must set)** | Recipient(s), comma-separated. Both DAGs fail fast if unset. |
| `db_maintenance_enable_reindex` | `false` | Enable auto `REINDEX CONCURRENTLY`. `false` = report-only (opt-in — the heavy action). |
| `db_maintenance_enable_freeze` | `true` | Auto `VACUUM (FREEZE)` for wraparound. Set `false` to force report-only. |
| `db_maintenance_enable_analyze` | `true` | Auto `ANALYZE` for stale stats. Set `false` to force report-only. |
| `db_maintenance_reindex_bloat_pct` | `30` | Reindex a table when est. index bloat ≥ this % (top of the 20–30% band; 20 thrashed churn tables). |
| `db_maintenance_reindex_min_mb` | `100` | Ignore indexes smaller than this (estimate noise). |
| `db_maintenance_wraparound_warn_pct` | `60` | **Act**: `VACUUM (FREEZE)` a public table when its wraparound age ≥ this %. |
| `db_maintenance_wraparound_alert_pct` | `45` | **WARNING** when DB-level wraparound ≥ this % **of the ~2.1B real limit** (autovacuum falling behind). |
| `db_maintenance_wraparound_crit_pct` | `70` | **ATTENTION** when DB-level wraparound ≥ this % of the ~2.1B real limit (near the failsafe/wall). |
| `db_maintenance_stale_mods` | `1000000` | Analyze/alert when a table has ≥ this many unanalyzed changes. |
| `db_maintenance_lock_timeout` | `30s` | Max lock wait per online action. |
| `db_maintenance_block_txn_min` | `10` | A txn older than this (min) = "blocker". |

### Testing / forcing an action
Lower the threshold to `0` so every table qualifies, turn the matching flag on, trigger,
then **reset afterwards**:

| Action | Set | Reset to |
|---|---|---|
| Reindex | `reindex_bloat_pct=0`, `reindex_min_mb=0`, `enable_reindex=true` | `30` / `100` / `false` |
| Freeze | `wraparound_warn_pct=0`, `enable_freeze=true` | `60` / `false` |
| Analyze | `stale_mods=0`, `enable_analyze=true` | `1000000` / `false` |

All actions are online; the email's **Reindex / Freeze / Analyze** lines show what each did.

---

## DAG 2 — `system_airflow_db_cleanup_v2` (WEEKLY, Saturday 12:00 UTC)

Retention-based row cleanup so tables don't grow unbounded. Runs Saturday to stay
off weekday integration runs.

- **Cleans only tables that do NOT cascade from `dag_run`:** `dag_run`, `job`, `log`,
  `session`, `trigger`. `dag_run` has `ON DELETE CASCADE`, so deleting it auto-removes
  `task_instance`, `xcom`, `task_fail`, `rendered_task_instance_fields`, etc. — cleaning
  those directly would be redundant.
- **Chunked deletes** (default 5000/chunk) with per-chunk `ANALYZE`.
- **Retention:** global `airflow_db_cleanup__max_db_entry_age_in_days` (default 30).
- **Email:** before → after per-table stats (rows deleted, size), success + failure,
  `[region-env]` subject, same `db_maintenance_alert_email` recipient.

| Variable | Default | Meaning |
|---|---|---|
| `airflow_db_cleanup__max_db_entry_age_in_days` | `30` | Retention window (days), all tables. |
| `airflow_db_cleanup_chunk_size` | `5000` | Rows deleted per chunk. |
| `db_maintenance_alert_email` | (must set) | Recipient(s). |

---

## One-time autovacuum tuning (applied via SQL, not a DAG)

Per-table overrides so autovacuum keeps up on the high-churn tables (kept in
`autovaccum_update.sql`). Metadata-only, no downtime:

- `task_instance`, `dag_run`: `vacuum_scale_factor=0.01, analyze_scale_factor=0.005`
- `job`: `0.02 / 0.01` · `xcom`: `0.05 / 0.02`

This is why ANALYZE/VACUUM don't need scheduling — autoanalyze/autovacuum fire on
% change, continuously.

### Recommended: `idle_in_transaction_session_timeout` (DB-level, one-time)

Set this on the metadata DB (e.g. **10–15 min**) so PostgreSQL itself auto-terminates
**leaked idle-in-transaction sessions** — the usual thing that blocks `REINDEX
CONCURRENTLY` and pins the xmin autovacuum needs to advance the frozen horizon:

```sql
ALTER DATABASE <airflow_db> SET idle_in_transaction_session_timeout = '15min';
```

It only kills sessions sitting **idle inside an open transaction** — it does **not**
touch a legitimately-running query. With this in place, blockers clear themselves, the
reindex reliably gets its window on the next run, and no one has to terminate anything
by hand. This is the hands-off fix for "what if a transaction is stuck" — done at the DB
layer (hygiene), not by the DAG (which never kills sessions).

---

## Bloat detection & threshold (research-backed)

**Threshold = 30% estimated index bloat (top of the recommended band).**
- Recommended band from the community is **20–30%** ("worth addressing"); **AWS
  recommends REINDEX when bloat > 20%.** We originally ran **20%** (the aggressive end)
  but real fleet data showed the high-churn tables land right in the 20–30% range
  *immediately* after a rebuild (`job` → ~30%, `dag_run` → ~21%) — they update an
  indexed column on nearly every write so the index re-bloats to its natural floor at
  once, and the cheap estimate over-reads by ~10% (fillfactor). At 20% those tables
  stayed "over threshold" **every run → reindexed daily for hours, reclaiming almost
  nothing** (thrash). **30% lets them rest** while still catching genuinely bloated
  indexes. *(For the very highest-churn tables even 30% may not fully stop daily
  re-reindex — a per-table reindex **cooldown** is the complete fix; see below.)*

**Detection = cheap catalog ESTIMATE, not `pgstattuple`.**
- The *accurate* measure is `pgstattuple`'s `pgstatindex(avg_leaf_density)`
  (bloat = 100 − avg_leaf_density) — but it **reads the entire index** to compute
  it. Running that daily on a 284 GB index = reading 284 GB/day just to measure,
  which is far more expensive than the occasional wasted reindex we accept. So we
  use the cheap estimate (`pg_class.reltuples` + `pg_stats` widths), ~±10%.
- At 30% on a ±10% estimate, some reindexes will hit not-fully-bloated indexes —
  **accepted deliberately** (aggressive, online, RDS headroom).
- If exact numbers are ever needed: `CREATE EXTENSION pgstattuple` (no reboot) and
  use `pgstatindex` — but **don't** run it daily on the huge indexes.

**Two gates (both must pass):** estimated bloat % ≥ 20 **AND** index size ≥
`db_maintenance_reindex_min_mb` (100 MB). The size floor skips tiny indexes where
the estimate is pure noise and a rebuild is pointless.

Sources: [AWS/community reindex >20% & fillfactor/re-bloat — CYBERTEC](https://www.cybertec-postgresql.com/en/should-i-rebuild-my-postgresql-index/),
[Index bloat: identify & resolve (kendralittle)](https://kendralittle.com/2025/12/01/index-bloat-postgres-why-it-matters-how-to-identify-and-resolve/),
[PostgreSQL bloat monitoring (pghealth.io)](https://pghealth.io/blog/postgresql-bloat-monitoring),
[Index maintenance strategy (oneuptime)](https://oneuptime.com/blog/post/2026-01-30-postgresql-index-maintenance/view).

---

## Key decisions (and why)

- **No `VACUUM FULL` automation** — it takes an exclusive lock (downtime), and only
  returns disk to the OS (which we don't care about — RDS). Online REINDEX fixes the
  performance problem (index bloat). A one-time deep `VACUUM FULL` in a planned window
  is fine as a reset, but not recurring.
- **Per-table reindex**, not per-index — our churn tables' indexes bloat together.
- **Bloat % metric**, not absolute size or index/heap ratio — scale/table-independent.
- **30% threshold** — top of the community 20–30% band; 20% thrashed high-churn tables (they re-bloat to ~20–30% instantly), so we raised it.
- **Reindex default OFF** — monitor/alert first; enable per-region when trusted.
- **Daily schedule** — daily proactive monitoring; reindex still gated (fires only when bloated).
- **Blocked reindex = flag, not fail** — keep the health report, avoid daily false-failure noise.
- **Monitor DAG removed** — its checks folded into this daily email; blockers cleared manually.

---

## Ops runbook

1. **Deploy** both DAGs and set `db_maintenance_alert_email`. **Freeze + analyze are on by default** — no other variables needed for them.
2. **Watch the daily email** for a run or two — confirm bloat %, wraparound, blockers look right. (Freeze/analyze are already acting; only reindex is still report-only.)
3. **One-time deep clean** (optional): support runs `VACUUM (FULL, ANALYZE)` on the bloated big tables in a planned maintenance window (offline). This is a one-off reset, communicated to customers.
4. **Enable reindex** per env once you've glanced at the first report (it's the heavy one):
   `db_maintenance_enable_reindex=true`. It then reindexes bloated tables online and reports
   what it did. (To *disable* freeze/analyze somewhere, set their variables to `false`.)
5. **Respond to ATTENTION alerts:**
   - *wraparound high, freeze disabled* → enable `db_maintenance_enable_freeze` (it'll `VACUUM (FREEZE)` the old tables next run), or run it once manually.
   - *long/idle txn* → identify and clear it (it pins the xmin autovacuum needs and makes `REINDEX CONCURRENTLY` wait; freeze still runs and does what it can). This is the only one you may need to act on by hand — and setting `idle_in_transaction_session_timeout` (see above) makes PostgreSQL auto-clear the leaked-idle ones so you usually won't have to.
   - *stale stats, analyze disabled* → enable `db_maintenance_enable_analyze`.
   - *bloat, reindex disabled* → enable `db_maintenance_enable_reindex`.

## Out of scope / not installed
- `VACUUM FULL` automation (downtime; unnecessary — see decisions).
- `pgstattuple` (exact bloat) — **deliberately not used** for the daily gate: it
  reads the whole index (too expensive daily on huge indexes). We use the cheap
  estimate instead. Install it only for one-off exact checks (`CREATE EXTENSION
  pgstattuple`, no reboot).
- `pg_stat_statements` (query-level timing) — not installed (needs a reboot).
