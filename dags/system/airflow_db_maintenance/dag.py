"""
### Airflow Metadata DB Maintenance

ONE DAG, run DAILY, that (a) proactively MONITORS metadata-DB health A-Z and (b)
reindexes bloated indexes online. It ALWAYS sends exactly ONE consolidated email
per run with a clear status banner at the top (severity = "does a human need to act?"):
  * ATTENTION (red)  — action required; bullet list of what to do, OR
  * WARNING (amber)  — review only; self-healing or a config choice (e.g. wraparound
                       that autovacuum will fix on its own) — "check with your DBA if it persists", OR
  * HEALTHY (green)  — no action needed.
The subject carries the same level, so you can filter: ATTENTION = act, WARNING = skim,
HEALTHY = ignore. Everything is in that one email — no silence, no flood of separate alerts.

=============================================================================
DESIGN DECISIONS (agreed — documented so the reasoning is clear)
=============================================================================

WHY THIS DAG EXISTS
  Autovacuum (tuned per-table) already handles VACUUM + ANALYZE continuously, so
  dead tuples and planner stats stay healthy on their own. The ONE thing
  autovacuum CANNOT do is shrink an index whose FILE has bloated. Over months of
  churn (state transitions on task_instance, heartbeats on job, mass deletes from
  the cleanup DAG, etc.) index files grow and never shrink -> slow scans -> the
  scheduler lags. Only REINDEX fixes that. So this DAG = proactive monitoring +
  the automated online REINDEX that autovacuum can't do. NO VACUUM FULL (that only
  returns disk to the OS, needs downtime, and we don't care about disk — RDS).

MONITORING (A-Z, read-only, every run) — surfaced in the one email:
  * index bloat % per table       (reindex candidates)
  * dead tuples + autovacuum age   (is autovacuum keeping up)
  * planner-stats staleness        (mod_since_analyze / last_autoanalyze)
  * XID + MultiXact wraparound age (as % of the ~2.1B read-only limit; healthy is
    single digits — a DB merely riding the autovacuum force-freeze trigger is ~9%)
  * long / idle-in-transaction     (the root cause of the original bloat)
  Any of these tripping -> the email banner flags it: ATTENTION (red) if a human must
  act, else WARNING (amber) if it's self-healing / a config choice (see banner note below).

AUTOMATION SCOPE (important) -> THREE automated corrective ACTIONS, all ONLINE
  (no downtime), each independently gated by its own flag + threshold, each
  auto-detecting the tables it acts on (no hardcoded table names):
    1. REINDEX  -> `REINDEX TABLE CONCURRENTLY` when index BLOAT % >= bloat_pct.
                   Fixes index bloat (autovacuum CANNOT shrink an index file).
                   Gate: db_maintenance_enable_reindex + db_maintenance_reindex_bloat_pct (30).
    2. FREEZE   -> `VACUUM (FREEZE)` on tables whose relfrozenxid/relminmxid age
                   >= wraparound_warn_pct of the freeze limit. Fixes XID/MultiXact
                   wraparound (advances the frozen horizon). Online, SHARE UPDATE
                   EXCLUSIVE lock only (reads+writes continue).
                   Gate: db_maintenance_enable_freeze + db_maintenance_wraparound_warn_pct (60).
    3. ANALYZE  -> `ANALYZE` on tables with >= stale_mods unanalyzed changes.
                   Fixes stale planner stats (backstop for when autoanalyze lags).
                   Gate: db_maintenance_enable_analyze + db_maintenance_stale_mods (1,000,000).
  FREEZE + ANALYZE default ON (safe, protective, run unattended); REINDEX defaults OFF
  (opt-in — the heavy one, rebuilds large indexes). Only REINDEX skips on a sustained (> block_txn_min)
  blocking txn — REINDEX CONCURRENTLY would WAIT on it and, in this one-run-at-a-time DAG,
  a multi-hour wait would stall freeze/analyze/monitoring too, so it skips + retries.
  FREEZE and ANALYZE always run when enabled (they never wait on a txn).
  STILL monitor + ALERT ONLY (NOT auto-acted):
    - long/idle-txn blockers -> ALERT (pg_terminate_backend is destructive; clear manually).
    - dead tuples / vac lag  -> autovacuum handles continuously; ALERT if it lags.
  NOT automatable without DOWNTIME (the one true exception):
    - VACUUM FULL (heap bloat / disk reclaim) -> takes an ACCESS EXCLUSIVE lock =
      downtime, and only returns disk to the OS (which we don't care about, RDS).
      One-time, in a planned window, by support. The 3 online actions above cover
      every PERFORMANCE problem; VACUUM FULL is never needed for that.

WHICH TABLES (for reindex) -> ALL of them, gated. We measure bloat on every
  table's indexes and only reindex those over threshold; a size floor
  (V_MIN_INDEX_MB) skips tiny indexes where the estimate is noise.

METRIC -> index BLOAT % (estimated from catalog stats, no extension). Scale- and
  table-independent. Approximate (+/-10%) — good enough to gate a reindex.

THRESHOLD -> 30% (top of the community 20-30% "worth addressing" band). We ORIGINALLY
  ran 20% (the aggressive end) but real fleet data showed our high-churn tables settle
  right around 20-30% IMMEDIATELY after a rebuild (job -> ~30%, dag_run -> ~21%): they
  update an indexed column on nearly every write, so the index re-bloats to its natural
  floor at once, AND the cheap estimate over-reads by ~10% (fillfactor). At 20% those
  tables stayed "over threshold" every run -> reindexed daily for hours, reclaiming
  almost nothing (thrash). 30% lets them rest while still catching genuinely bloated
  indexes. Detection is the cheap catalog ESTIMATE (not pgstattuple) — see "DETECTION"
  below. Refs: AWS (reindex >20%), CYBERTEC (fillfactor / re-bloat).
  NOTE: for the very highest-churn tables even 30% may not fully stop daily re-reindex
  (job sits ~30% right after a rebuild); a per-table reindex cooldown is the complete fix.

DETECTION -> cheap catalog ESTIMATE (pg_class.reltuples + pg_stats widths), NOT
  pgstattuple. pgstattuple/pgstatindex is more accurate but READS THE ENTIRE INDEX
  to measure — running that daily on a 284 GB index would read 284 GB/day just to
  check bloat, far more expensive than the occasional wasted reindex we accept.
  The estimate is ~+/-10%; combined with the 30% threshold we accept
  that some reindexes hit not-fully-bloated indexes (deliberate). If exact numbers
  are ever needed, `CREATE EXTENSION pgstattuple` (no reboot) + pgstatindex, but
  don't run it daily on huge indexes.

SCHEDULE -> DAILY (03:00 UTC). Daily = proactive: you see a problem within a day,
  not a week. The schedule drives MONITORING frequency; it does NOT mean daily
  reindex — reindex is gated, so it only fires the day a table actually crosses
  the bloat threshold (~every few months). Cheap read-only checks every day.

DEFAULT OFF -> reindex is DISABLED by default (V_ENABLE_REINDEX=false) = monitor +
  email only, no reindex. Set `db_maintenance_enable_reindex = true` to enable.
  Online (REINDEX CONCURRENTLY, no downtime), skipped if a long/idle txn is open.

TESTING / FORCE AN ACTION -> lower a threshold to 0 so every table qualifies,
  turn the matching flag on, trigger, then RESET afterwards:
    * reindex -> `reindex_bloat_pct=0` + `reindex_min_mb=0` + `enable_reindex=true`
                 (reset to 30 / 100 / false).
    * freeze  -> `wraparound_warn_pct=0` + `enable_freeze=true`
                 (reset to 60 / false). Runs VACUUM (FREEZE) on all public tables.
    * analyze -> `stale_mods=0` + `enable_analyze=true`
                 (reset to 1000000 / false). Runs ANALYZE on all tables.
  All are online; the email's Reindex/Freeze/Analyze lines show what each did.

REINDEX GRANULARITY -> per-TABLE (REINDEX TABLE CONCURRENTLY), NOT per-index.
  DELIBERATE: our churn tables update an INDEXED column on nearly every change
  (state, latest_heartbeat) -> no HOT updates -> ALL of a table's indexes bloat
  together, so per-index would rebuild them all anyway, for more risk. Rejected.

BLOCKERS -> a long/idle-in-transaction session makes a reindex stall, so if one is
  open the reindex is SKIPPED and the email banner flags it (subject + body).
  There is NO auto-cleaner (the monitor DAG was removed) — clear it MANUALLY. We
  FLAG, we do NOT fail the task (so the health report still goes out and we don't
  fire a false FAILURE email every day for a known condition).

EMAIL -> rail.EmailOperator. ONE consolidated email every run (success) with the
  status banner + monitoring tables + reindex before/after; a separate FAILURE
  email only if a task actually errors. Subject: [region-env] + STATUS. Recipient:
  `db_maintenance_alert_email` Variable (comma-separated for multiple).

Companion: system_airflow_db_cleanup_v2 (weekly row retention — separate job).
"""
from datetime import datetime, timedelta
import logging
import os

import airflow
from airflow.models import Variable
from airflow.settings import engine
from sqlalchemy import text
import rail

log = logging.getLogger(__name__)

# ---- Config (all overridable via Airflow Variables) -------------------------
V_ENABLE_REINDEX = "db_maintenance_enable_reindex"      # default FALSE — opt-in (heavy: rebuilds large indexes)
V_ENABLE_FREEZE  = "db_maintenance_enable_freeze"       # default TRUE  — auto VACUUM (FREEZE) near wraparound (online, protective)
V_ENABLE_ANALYZE = "db_maintenance_enable_analyze"      # default TRUE  — auto ANALYZE stale tables (online, cheap)
V_BLOAT_PCT      = "db_maintenance_reindex_bloat_pct"   # default 30 (top of the 20-30% band; 20 thrashed churn tables that re-bloat to ~20-30% instantly)
V_MIN_INDEX_MB   = "db_maintenance_reindex_min_mb"      # default 100
V_LOCK_TIMEOUT   = "db_maintenance_lock_timeout"        # default 30s
V_BLOCK_TXN_MIN  = "db_maintenance_block_txn_min"       # default 10 (min) — long/idle txn alert
V_WRAP_WARN_PCT  = "db_maintenance_wraparound_warn_pct" # default 60 — ACT: proactively freeze a PUBLIC table when its
                                                        #   age reaches this % of autovacuum_freeze_max_age (the force-freeze trigger)
# Wraparound ALERT thresholds are measured against the REAL read-only limit (~2.1B XIDs),
# NOT autovacuum_freeze_max_age. A healthy DB rides up to ~100% of the force-freeze
# trigger (~200M) = only ~9% of the real limit, so alerting on the trigger cries wolf.
V_WRAP_ALERT_PCT = "db_maintenance_wraparound_alert_pct" # default 45 (% of the ~2.1B real limit) — WARNING: autovacuum falling behind
V_WRAP_CRIT_PCT  = "db_maintenance_wraparound_crit_pct"  # default 70 (% of the ~2.1B real limit) — ATTENTION: near the failsafe/wall
WRAP_HARD_LIMIT  = 2_147_483_648                        # 2^31 — XID/MultiXact age at which Postgres goes read-only
V_STALE_MODS     = "db_maintenance_stale_mods"          # default 1,000,000 unanalyzed changes

# Dedicated recipient (comma-separated for multiple). Set `db_maintenance_alert_email`.
ALERT_EMAIL = "{{ var.value.db_maintenance_alert_email }}"


def _region_env():
    return f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'dev')}"


def _get(name, default):
    return Variable.get(name, default_var=default)


def _reindex_enabled():
    return str(_get(V_ENABLE_REINDEX, "false")).lower() == "true"


def _freeze_enabled():
    # Default ON: light lock, can't stall, only fires near wraparound — protective,
    # safe to run unattended. Set the Variable to "false" to force report-only.
    return str(_get(V_ENABLE_FREEZE, "true")).lower() == "true"


def _analyze_enabled():
    # Default ON: cheap, no lock, backstop-only (fires rarely). Set "false" to disable.
    return str(_get(V_ENABLE_ANALYZE, "true")).lower() == "true"


def _fetch(sql, **params):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        return conn.execute(text(sql), params).mappings().all()


def _maint_conn():
    conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    conn.exec_driver_sql(f"SET lock_timeout = '{_get(V_LOCK_TIMEOUT, '0')}'")
    conn.exec_driver_sql("SET statement_timeout = 0")   # never kill a long reindex
    # TCP keepalives: REINDEX CONCURRENTLY sends no data back to the client for hours,
    # so VPC/RDS proxy treats the connection as idle and drops it. Keepalives prevent that.
    conn.exec_driver_sql("SET tcp_keepalives_idle = 60")      # start keepalives after 60s idle
    conn.exec_driver_sql("SET tcp_keepalives_interval = 10")  # retry every 10s
    conn.exec_driver_sql("SET tcp_keepalives_count = 5")      # drop after 5 missed keepalives
    return conn


def _sz(b):
    b = float(b or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} PB"


def _snapshot():
    """Per-table current health (JSON-serializable). Used for the report and the
    'before' capture (to show index size reclaimed after a reindex)."""
    rows = _fetch(
        """
        SELECT relname,
               n_live_tup AS rows,
               round(100.0*n_dead_tup/nullif(n_live_tup+n_dead_tup,0),1) AS dead_pct,
               pg_relation_size(relid)       AS heap_bytes,
               pg_indexes_size(relid)        AS index_bytes,
               pg_total_relation_size(relid) AS total_bytes,
               round((extract(epoch FROM now()-last_autovacuum)/3600)::numeric,1)  AS hrs_since_vac,
               round((extract(epoch FROM now()-last_autoanalyze)/3600)::numeric,1) AS hrs_since_analyze,
               n_mod_since_analyze AS mod_since_analyze
        FROM pg_stat_user_tables
        WHERE pg_total_relation_size(relid) > 8192
        ORDER BY pg_total_relation_size(relid) DESC
        """
    )
    return [{"relname": r["relname"], "rows": int(r["rows"] or 0),
             "dead_pct": float(r["dead_pct"] or 0), "heap_bytes": int(r["heap_bytes"] or 0),
             "index_bytes": int(r["index_bytes"] or 0), "total_bytes": int(r["total_bytes"] or 0),
             "hrs_since_vac": float(r["hrs_since_vac"] or 0),
             "hrs_since_analyze": float(r["hrs_since_analyze"] or 0),
             "mod_since_analyze": int(r["mod_since_analyze"] or 0)}
            for r in rows]


def _index_bloat(min_mb):
    """Estimated per-table index bloat % (no extension); tables with total index
    size >= min_mb only. APPROXIMATE — good enough to gate a reindex."""
    return _fetch(
        """
        SELECT t.relname AS table_name,
               sum(pg_relation_size(i.indexrelid)) AS index_bytes,
               round((100.0 * sum(greatest(pg_relation_size(i.indexrelid) - w.est_bytes, 0))
                     / nullif(sum(pg_relation_size(i.indexrelid)), 0))::numeric, 0) AS bloat_pct
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_class t  ON t.oid  = i.indrelid
        JOIN pg_namespace n ON n.oid = ic.relnamespace
        JOIN LATERAL (
            SELECT ceil(t2.reltuples * (coalesce(sum(st.avg_width), 8) + 8) / 8168.0) * 8192 AS est_bytes
            FROM pg_class t2
            LEFT JOIN pg_attribute a ON a.attrelid = i.indexrelid AND a.attnum > 0
            LEFT JOIN pg_stats st ON st.schemaname = n.nspname AND st.tablename = t.relname AND st.attname = a.attname
            WHERE t2.oid = i.indrelid
            GROUP BY t2.reltuples
        ) w ON true
        -- reltuples > 0 guards two cases: a never-analyzed table reports
        -- reltuples = -1 (PG>=14) -> negative est_bytes -> false 100% bloat ->
        -- spurious reindex; and a genuinely empty table (0) has no bloat to fix.
        -- Either way we can't estimate, so skip it (shows "—" in the report).
        WHERE n.nspname = 'public' AND ic.relkind = 'i' AND t.reltuples > 0
        GROUP BY t.relname
        HAVING sum(pg_relation_size(i.indexrelid)) >= :min_bytes
        ORDER BY index_bytes DESC
        """,
        min_bytes=int(min_mb) * 1024 * 1024,
    )


def _bloated_tables():
    pct = float(_get(V_BLOAT_PCT, 30))
    min_mb = int(_get(V_MIN_INDEX_MB, 100))
    return [(r["table_name"], r["bloat_pct"]) for r in _index_bloat(min_mb)
            if (r["bloat_pct"] or 0) >= pct]


def _old_tables():
    """Tables whose relfrozenxid / relminmxid age is >= warn% of the freeze limit
    — i.e. the ones DRIVING wraparound. Found dynamically (no hardcoded table);
    these get an online VACUUM (FREEZE). Public user tables only (the churn
    drivers); system catalogs are frozen by autovacuum / owned by rdsadmin."""
    warn = float(_get(V_WRAP_WARN_PCT, 60))
    return _fetch(
        """
        SELECT c.relname,
               greatest(round(100.0*age(c.relfrozenxid)/xm.xid_max, 0),
                        round(100.0*mxid_age(c.relminmxid)/xm.mxid_max, 0)) AS pct
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN (SELECT current_setting('autovacuum_freeze_max_age')::bigint            AS xid_max,
                           current_setting('autovacuum_multixact_freeze_max_age')::bigint  AS mxid_max) xm
        WHERE c.relkind IN ('r','m') AND n.nspname = 'public'
          AND greatest(100.0*age(c.relfrozenxid)/xm.xid_max,
                       100.0*mxid_age(c.relminmxid)/xm.mxid_max) >= :warn
        ORDER BY pct DESC
        """,
        warn=warn,
    )


def _stale_tables():
    """Tables with >= stale_mods unanalyzed changes — planner stats are behind.
    Found dynamically; these get an online ANALYZE."""
    mods = int(_get(V_STALE_MODS, 1_000_000))
    return [r["relname"] for r in _fetch(
        "SELECT relname FROM pg_stat_user_tables WHERE n_mod_since_analyze >= :m "
        "ORDER BY n_mod_since_analyze DESC", m=mods)]


def _wraparound():
    # Only the DB this DAG is connected to (the Airflow metadata DB). Wraparound
    # is technically a cluster-wide counter, but the system DBs (postgres,
    # template1) are static and autovacuum freezes them instantly — this churn DB
    # is always the oldest, so it's the faithful signal. Each Airflow instance has
    # its own cluster; we never see (or need) another instance's databases.
    return _fetch(
        """
        SELECT datname,
               age(datfrozenxid)                                              AS xid_age,
               mxid_age(datminmxid)                                           AS mxid_age,
               current_setting('autovacuum_freeze_max_age')::bigint           AS xid_max,
               current_setting('autovacuum_multixact_freeze_max_age')::bigint AS mxid_max
        FROM pg_database WHERE datname = current_database()
        """
    )


def _xmin_pins():
    """Things pinning the global xmin — the ONLY reason an elevated wraparound won't
    self-heal (they stop autovacuum AND our freeze from advancing the frozen
    horizon). Empty list => nothing is holding it back, so a high wraparound is
    self-healing (WARNING/info, not ATTENTION). Covers all three holders: a long/idle
    txn, a prepared (2PC) txn, and an inactive replication slot still pinning an xmin
    — the two latter do NOT show in the pg_stat_activity session list."""
    pins = []
    for t in _long_txns():
        pins.append(f"txn pid {t['pid']} ({t['state']}, {t['min']} min)")
    for r in _fetch("SELECT gid FROM pg_prepared_xacts"):
        pins.append(f"prepared xact '{r['gid']}'")
    for r in _fetch("SELECT slot_name FROM pg_replication_slots "
                    "WHERE active = false AND (xmin IS NOT NULL OR catalog_xmin IS NOT NULL)"):
        pins.append(f"inactive replication slot '{r['slot_name']}'")
    return pins


def _long_txns():
    # Sessions in a transaction open longer than block_txn_min — the ones that
    # actually matter: they make REINDEX CONCURRENTLY wait and pin the xmin that
    # autovacuum needs to advance the frozen horizon. We deliberately do NOT list
    # short-lived idle-in-transaction sessions: a busy Airflow DB always has a few
    # (scheduler/worker SELECTs sitting momentarily idle in a txn), and flagging
    # those on any age kept the email permanently yellow for no real reason.
    mins = int(_get(V_BLOCK_TXN_MIN, 10))
    return _fetch(
        """
        SELECT pid, coalesce(usename,'') AS usename, state,
               round((extract(epoch FROM now()-xact_start)/60)::numeric,1) AS min,
               left(coalesce(query,''),80) AS query
        FROM pg_stat_activity
        WHERE backend_type='client backend' AND pid <> pg_backend_pid() AND xact_start IS NOT NULL
          AND now()-xact_start > (:m || ' minutes')::interval
        ORDER BY xact_start
        """,
        m=mins,
    )


def _has_blocking_txn():
    # Same set as _long_txns(): a transaction open > block_txn_min. Only these make
    # REINDEX CONCURRENTLY wait long enough to matter; transient idle-in-txn churn
    # does not, so it must NOT gate the action (that was the bug where reindex
    # "never triggered" despite the enable flag). One source of truth for both the
    # gate and the email so they can never disagree.
    return len(_long_txns()) > 0


def _tables_with_active_reindex():
    """Tables with an in-flight REINDEX CONCURRENTLY or CREATE INDEX CONCURRENTLY right now
    (pg_stat_progress_create_index, PG 12+). Issuing a second REINDEX CONCURRENTLY on the
    same table while one is already running causes an immediate lock conflict / deadlock."""
    rows = _fetch(
        "SELECT DISTINCT c.relname FROM pg_stat_progress_create_index p "
        "JOIN pg_class c ON c.oid = p.relid"
    )
    return {r["relname"] for r in rows}


def _tables_with_invalid_indexes():
    """Tables that already have a leftover invalid index (_ccnew/_ccold) in the public schema.
    A _ccnew means the prior REINDEX CONCURRENTLY was interrupted and left a partial shadow
    index. Starting a new REINDEX CONCURRENTLY on top of it will try to acquire the same
    ShareUpdateExclusiveLock that the leftover holds, causing another deadlock. Must be
    cleared with DROP INDEX CONCURRENTLY before retrying.
    Excludes all invalid indexes on tables where ANY index build is currently in progress
    (pg_stat_progress_create_index). REINDEX TABLE CONCURRENTLY creates ALL _ccnew indexes
    upfront (all indisvalid=false) then builds them sequentially — only the one actively being
    built appears in pg_stat_progress_create_index, but the rest are not orphans either. A
    table-level exclusion (t.oid NOT IN ...) is the correct guard."""
    rows = _fetch(
        "SELECT DISTINCT t.relname FROM pg_index i "
        "JOIN pg_class ic ON ic.oid = i.indexrelid "
        "JOIN pg_class t  ON t.oid  = i.indrelid "
        "JOIN pg_namespace n ON n.oid = ic.relnamespace "
        "WHERE n.nspname = 'public' AND NOT i.indisvalid "
        "AND t.oid NOT IN (SELECT relid FROM pg_stat_progress_create_index)"
    )
    return {r["relname"] for r in rows}


def _invalid_indexes():
    """READ-ONLY. Genuinely orphaned invalid indexes — i.e. the PostgreSQL backend that
    was building them is NO LONGER running (not in pg_stat_progress_create_index).
    Suffix meaning (per PG docs, https://www.postgresql.org/docs/current/sql-reindex.html):
      `_ccnew*` = transient rebuild never finished — auto-dropped by this DAG before retrying.
      `_ccold*` = rebuild succeeded, old index couldn't be swapped out — auto-dropped, no re-run.
    Table-level exclusion (t.oid NOT IN ...): REINDEX TABLE CONCURRENTLY creates ALL _ccnew
    indexes upfront (all indisvalid=false) then builds them one by one — only the active one
    appears in pg_stat_progress_create_index, but ALL of them belong to the same live backend.
    Excluding at table level prevents false ATTENTION on the queued-but-not-yet-active ones."""
    return _fetch(
        """
        SELECT ic.relname AS index_name,
               t.relname  AS table_name,
               pg_relation_size(i.indexrelid) AS index_bytes,
               CASE WHEN ic.relname ~ '_ccnew[0-9]*$' THEN 'ccnew'
                    WHEN ic.relname ~ '_ccold[0-9]*$' THEN 'ccold'
                    ELSE 'invalid' END AS kind
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_class t  ON t.oid  = i.indrelid
        JOIN pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND NOT i.indisvalid
          AND t.oid NOT IN (SELECT relid FROM pg_stat_progress_create_index)
        ORDER BY pg_relation_size(i.indexrelid) DESC
        """
    )


def _inprogress_invalid_indexes():
    """READ-ONLY. Invalid indexes on tables where a REINDEX / CREATE INDEX is STILL RUNNING
    on the PostgreSQL backend (present in pg_stat_progress_create_index).
    When the Airflow client connection drops mid-reindex, PostgreSQL does NOT know — it only
    detects a disconnect when it next tries to write to the client. Since REINDEX CONCURRENTLY
    never writes output, the backend keeps running silently after the client is gone.
    These indexes are NOT orphans. Do NOT drop them — the build will finish on its own."""
    return _fetch(
        """
        SELECT ic.relname AS index_name,
               t.relname  AS table_name,
               pg_relation_size(i.indexrelid) AS index_bytes,
               CASE WHEN ic.relname ~ '_ccnew[0-9]*$' THEN 'ccnew'
                    WHEN ic.relname ~ '_ccold[0-9]*$' THEN 'ccold'
                    ELSE 'invalid' END AS kind
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_class t  ON t.oid  = i.indrelid
        JOIN pg_namespace n ON n.oid = ic.relnamespace
        WHERE n.nspname = 'public' AND NOT i.indisvalid
          AND t.oid IN (SELECT relid FROM pg_stat_progress_create_index)
        ORDER BY t.relname, pg_relation_size(i.indexrelid) DESC
        """
    )


# =============================================================================
def snapshot_before(**context):
    # Fail fast (visibly, in the Airflow UI) if the recipient isn't configured.
    # Otherwise every email renders an empty `to` and is silently dropped — incl.
    # the failure email — which would defeat the "always exactly one email" guarantee.
    if not str(_get("db_maintenance_alert_email", "")).strip():
        raise ValueError("Variable 'db_maintenance_alert_email' is not set. Set it "
                         "(comma-separated for multiple) so maintenance alerts can be delivered.")
    context["ti"].xcom_push(key="region_env", value=_region_env())
    # Capture the bloat estimate NOW, before any reindex, so build_report can show
    # est_bloat% as "before -> after" and the reclaim (or lack of it, on a
    # near-minimum index) is visible at a glance instead of a single number.
    context["ti"].xcom_push(
        key="bloat_before",
        value={r["table_name"]: float(r["bloat_pct"] or 0)
               for r in _index_bloat(int(_get(V_MIN_INDEX_MB, 100)))})
    return _snapshot()   # -> XCom


def reindex_if_bloated(**context):
    pct = float(_get(V_BLOAT_PCT, 30))
    bloated = _bloated_tables()
    context["ti"].xcom_push(key="bloated", value=[t for t, _ in bloated])
    context["ti"].xcom_push(key="reindexed", value=[])
    context["ti"].xcom_push(key="blocked", value=False)
    if not bloated:
        log.info("No table over %s%% estimated index bloat — nothing to reindex.", pct)
        return
    log.warning("Bloated (>= %s%%): %s", pct, bloated)
    if not _reindex_enabled():
        log.warning("[report-only] Would REINDEX %s. Set %s=true to enable.",
                    [t for t, _ in bloated], V_ENABLE_REINDEX)
        return
    if _has_blocking_txn():
        # No monitor DAG auto-clears blockers anymore — flag it in the email; a
        # human must clear the idle/long txn. Reindex retries next run.
        context["ti"].xcom_push(key="blocked", value=True)
        log.warning("Long/idle txn open — skipping reindex (would stall). CLEAR THE BLOCKER.")
        return
    active_reindex = _tables_with_active_reindex()
    done, skipped_active, skipped_drop_failed, dropped_invalid = [], [], [], []
    for tbl, b in bloated:
        if tbl in active_reindex:
            log.warning("SKIP REINDEX %s — already in progress (pg_stat_progress_create_index); retries next run.", tbl)
            skipped_active.append(tbl)
            continue

        # Auto-drop any leftover _ccnew / _ccold invalid indexes for this table before
        # reindexing. Per PG docs (https://www.postgresql.org/docs/current/sql-reindex.html):
        #   _ccnew = transient index from an interrupted REINDEX CONCURRENTLY — must be
        #            dropped, then REINDEX retried.
        #   _ccold = original index that could not be swapped out after a successful rebuild
        #            — safe to drop, no re-run needed.
        # Safety guarantee: indisvalid=false AND NOT EXISTS in pg_stat_progress_create_index
        # means PostgreSQL itself confirms the index is orphaned and not currently being built.
        # Invalid indexes consume write overhead on every INSERT/UPDATE even though the query
        # planner never uses them — dropping them is the correct and safe action per PG docs.
        leftovers = _fetch(
            "SELECT ic.relname AS index_name, "
            "       CASE WHEN ic.relname ~ '_ccnew[0-9]*$' THEN 'ccnew' "
            "            WHEN ic.relname ~ '_ccold[0-9]*$' THEN 'ccold' "
            "            ELSE 'invalid' END AS kind "
            "FROM pg_index i "
            "JOIN pg_class ic ON ic.oid = i.indexrelid "
            "JOIN pg_class t  ON t.oid  = i.indrelid "
            "JOIN pg_namespace n ON n.oid = ic.relnamespace "
            "WHERE n.nspname = 'public' AND NOT i.indisvalid AND t.relname = :tbl "
            "  AND t.oid NOT IN (SELECT relid FROM pg_stat_progress_create_index) "
            "ORDER BY pg_relation_size(i.indexrelid) DESC",
            tbl=tbl,
        )
        drop_failed = False
        for r in leftovers:
            idx = r["index_name"]
            q_idx = '"' + idx.replace('"', '""') + '"'
            conn = _maint_conn()
            try:
                log.warning("DROP INDEX CONCURRENTLY %s (orphaned %s on %s) ...", idx, r["kind"], tbl)
                conn.exec_driver_sql(f"DROP INDEX CONCURRENTLY {q_idx}")
                log.warning("DROP done: %s", idx)
                dropped_invalid.append({"index_name": idx, "table_name": tbl, "kind": r["kind"]})
            except Exception as e:
                log.error("DROP INDEX CONCURRENTLY failed for %s: %s — skipping REINDEX %s this run", idx, e, tbl)
                drop_failed = True
            finally:
                conn.close()
        if drop_failed:
            log.warning("SKIP REINDEX %s — could not drop all leftover invalid indexes; retries next run.", tbl)
            skipped_drop_failed.append(tbl)
            continue

        q = '"' + tbl.replace('"', '""') + '"'   # quote the identifier (catalog-sourced, but be safe)
        vac_conn = _maint_conn()
        try:
            # VACUUM before REINDEX: clears dead tuples so autovacuum won't trigger
            # for ~5+ hours (well beyond the REINDEX window), eliminating the deadlock
            # cycle where REINDEX and autovacuum compete for ShareUpdateExclusiveLock.
            # Also naturally waits for any running autovacuum to finish first
            # (same lock — blocks until clear, then proceeds). lock_timeout = 0 so
            # we wait as long as needed.
            log.warning("VACUUM %s (pre-REINDEX: wait for autovacuum, clear dead tuples) ...", tbl)
            vac_conn.exec_driver_sql(f"VACUUM {q}")
            log.warning("VACUUM done: %s", tbl)
        except Exception as e:
            log.error("VACUUM failed for %s: %s — skipping REINDEX this run (retries next run)", tbl, e)
            skipped_drop_failed.append(tbl)
            continue
        finally:
            vac_conn.close()

        # Reindex one index at a time (REINDEX INDEX CONCURRENTLY) instead of
        # the whole table at once. Each index takes 10-20 min — short enough that
        # autovacuum cannot accumulate enough dead tuples to trigger mid-build,
        # eliminating the ShareUpdateExclusiveLock deadlock cycle. Between indexes
        # we hold no locks, so autovacuum can freely run without conflict.
        indexes = [
            r["indexname"]
            for r in _fetch(
                "SELECT ic.relname AS indexname "
                "FROM pg_index i "
                "JOIN pg_class ic ON ic.oid = i.indexrelid "
                "JOIN pg_class t  ON t.oid  = i.indrelid "
                "JOIN pg_namespace n ON n.oid = ic.relnamespace "
                "WHERE n.nspname = 'public' AND i.indisvalid AND t.relname = :tbl "
                "ORDER BY pg_relation_size(i.indexrelid) DESC",
                tbl=tbl,
            )
        ]
        log.warning("REINDEX %s — %d index(es), largest first (est %s%% bloat)", tbl, len(indexes), b)
        any_succeeded = False
        for idx in indexes:
            qi = '"' + idx.replace('"', '""') + '"'
            for attempt in range(3):
                if attempt > 0:
                    # A deadlock left a _ccnew orphan — drop it, then VACUUM the
                    # table again before retrying. VACUUM naturally waits for
                    # autovacuum to fully exit (same lock), then clears dead tuples
                    # to zero, guaranteeing a clean window for the retry.
                    orphan = '"' + (idx + "_ccnew").replace('"', '""') + '"'
                    orp = _maint_conn()
                    try:
                        orp.exec_driver_sql(f"DROP INDEX CONCURRENTLY IF EXISTS {orphan}")
                    except Exception:
                        pass
                    finally:
                        orp.close()
                    log.warning("Deadlock on %s (attempt %d) — VACUUM then retry...", idx, attempt)
                    rc = _maint_conn()
                    try:
                        rc.exec_driver_sql(f"VACUUM {q}")
                    except Exception:
                        pass
                    finally:
                        rc.close()
                conn = _maint_conn()
                try:
                    log.warning("REINDEX INDEX CONCURRENTLY %s (attempt %d/3) ...", idx, attempt + 1)
                    conn.exec_driver_sql(f"REINDEX INDEX CONCURRENTLY {qi}")
                    log.warning("REINDEX INDEX done: %s", idx)
                    any_succeeded = True
                    break
                except Exception as e:
                    if "deadlock" in str(e).lower() and attempt < 2:
                        continue
                    log.error("REINDEX INDEX failed for %s: %s (retries next run)", idx, e)
                    break
                finally:
                    conn.close()
        if any_succeeded:
            done.append(tbl)
    context["ti"].xcom_push(key="reindexed", value=done)
    context["ti"].xcom_push(key="skipped_active", value=skipped_active)
    context["ti"].xcom_push(key="skipped_drop_failed", value=skipped_drop_failed)
    context["ti"].xcom_push(key="dropped_invalid", value=dropped_invalid)


def freeze_if_old(**context):
    """Automated online VACUUM (FREEZE) on the tables driving wraparound. Detect ->
    (gated only by the enable flag) act -> report. Takes just a SHARE UPDATE
    EXCLUSIVE lock (reads + writes continue).

    DELIBERATELY NOT gated on a blocking txn (unlike reindex): VACUUM (FREEZE) never
    WAITS on an open transaction — it freezes everything older than the oldest
    running xid and returns immediately. Skipping it would make ZERO progress;
    running it always advances the frozen horizon as far as it can, which is exactly
    what we want, especially near wraparound. If a long txn is pinning the oldest
    xid, the wraparound ATTENTION alert still tells a human to clear it (and the
    DB-level idle_in_transaction_session_timeout should auto-clear leaked ones)."""
    warn = float(_get(V_WRAP_WARN_PCT, 60))
    old = _old_tables()
    names = [r["relname"] for r in old]
    context["ti"].xcom_push(key="near_wrap", value=names)
    context["ti"].xcom_push(key="frozen", value=[])
    context["ti"].xcom_push(key="blocked", value=False)   # kept False: freeze is never gated on a txn
    if not old:
        log.info("No table >= %s%% of the freeze limit — nothing to freeze.", warn)
        return
    log.warning("Near wraparound (>= %s%%): %s", warn, names)
    if not _freeze_enabled():
        log.warning("[report-only] Would VACUUM (FREEZE) %s. Set %s=true to enable.", names, V_ENABLE_FREEZE)
        return
    done = []
    for tbl in names:
        conn = _maint_conn()
        try:
            q = '"' + tbl.replace('"', '""') + '"'
            log.warning("VACUUM (FREEZE) %s ... online", tbl)
            conn.exec_driver_sql(f"VACUUM (FREEZE) {q}")
            done.append(tbl)
        except Exception as e:
            log.error("VACUUM FREEZE failed for %s: %s", tbl, e)
        finally:
            conn.close()
    context["ti"].xcom_push(key="frozen", value=done)


def analyze_if_stale(**context):
    """Automated online ANALYZE on tables whose planner stats have fallen behind
    (>= stale_mods unanalyzed changes). Cheap, fast, no lock. Autovacuum normally
    handles this; this is the backstop for when autoanalyze is behind."""
    stale = _stale_tables()
    context["ti"].xcom_push(key="stale", value=stale)
    context["ti"].xcom_push(key="analyzed", value=[])
    if not stale:
        log.info("No table with stale planner stats — nothing to analyze.")
        return
    log.warning("Stale planner stats: %s", stale)
    if not _analyze_enabled():
        log.warning("[report-only] Would ANALYZE %s. Set %s=true to enable.", stale, V_ENABLE_ANALYZE)
        return
    done = []
    for tbl in stale:
        conn = _maint_conn()
        try:
            q = '"' + tbl.replace('"', '""') + '"'
            log.warning("ANALYZE %s ...", tbl)
            conn.exec_driver_sql(f"ANALYZE {q}")
            done.append(tbl)
        except Exception as e:
            log.error("ANALYZE failed for %s: %s", tbl, e)
        finally:
            conn.close()
    context["ti"].xcom_push(key="analyzed", value=done)


def build_report(**context):
    """Gather every signal, decide status, build the ONE consolidated email."""
    pct = float(_get(V_BLOAT_PCT, 30))
    warn = float(_get(V_WRAP_WARN_PCT, 60))
    db = _fetch("SELECT current_database() AS d")[0]["d"]
    label = f"{_region_env()} — {db}"

    before = context["ti"].xcom_pull(task_ids="snapshot_before") or []
    after = _snapshot()
    bloat_map = {r["table_name"]: float(r["bloat_pct"] or 0)                 # AFTER (now)
                 for r in _index_bloat(int(_get(V_MIN_INDEX_MB, 100)))}
    before_bloat_map = context["ti"].xcom_pull(task_ids="snapshot_before", key="bloat_before") or {}  # BEFORE
    bloated = context["ti"].xcom_pull(task_ids="reindex_if_bloated", key="bloated") or []
    reindexed = context["ti"].xcom_pull(task_ids="reindex_if_bloated", key="reindexed") or []
    blocked = context["ti"].xcom_pull(task_ids="reindex_if_bloated", key="blocked") or False
    skipped_active      = context["ti"].xcom_pull(task_ids="reindex_if_bloated", key="skipped_active")      or []
    dropped_invalid     = context["ti"].xcom_pull(task_ids="reindex_if_bloated", key="dropped_invalid")     or []
    skipped_drop_failed = context["ti"].xcom_pull(task_ids="reindex_if_bloated", key="skipped_drop_failed") or []
    near_wrap = context["ti"].xcom_pull(task_ids="freeze_if_old", key="near_wrap") or []
    frozen = context["ti"].xcom_pull(task_ids="freeze_if_old", key="frozen") or []
    freeze_blocked = context["ti"].xcom_pull(task_ids="freeze_if_old", key="blocked") or False
    stale = context["ti"].xcom_pull(task_ids="analyze_if_stale", key="stale") or []
    analyzed = context["ti"].xcom_pull(task_ids="analyze_if_stale", key="analyzed") or []

    bmap = {x["relname"]: x for x in before}
    amap = {x["relname"]: x for x in after}
    reclaimed_map = {t: (bmap.get(t, {}).get("index_bytes", 0), amap.get(t, {}).get("index_bytes", 0))
                     for t in reindexed}

    # ---- extra A-Z signals ----
    wrap = _wraparound()
    # % against the REAL read-only wall (~2.1B), not autovacuum_freeze_max_age — so a
    # DB merely riding the normal force-freeze trigger (~9% of the wall) doesn't alarm.
    worst_wrap = max((max(100.0 * w["xid_age"] / WRAP_HARD_LIMIT, 100.0 * w["mxid_age"] / WRAP_HARD_LIMIT)
                      for w in wrap), default=0.0)
    txns = _long_txns()
    invalid_idx    = _invalid_indexes()           # genuine orphans — backend no longer running
    inprogress_idx = _inprogress_invalid_indexes() # PG backend still running after client disconnect

    # ---- classify every signal into ATTENTION (act now) vs WARNING (review / check
    # with DBA; self-healing or a config choice). Drives the subject, banner, body. ----
    mins = int(_get(V_BLOCK_TXN_MIN, 10))
    attention, warning = [], []

    # Action blocked by a sustained txn -> ATTENTION (a human must clear it).
    if blocked or freeze_blocked:
        which = "/".join(x for x, on in (("reindex", blocked), ("freeze", freeze_blocked)) if on)
        attention.append(f"{which.upper()} BLOCKED — a transaction open > {mins} min is holding it up; skipped. "
                         f"Clear it (it also stops autovacuum from freezing, which drives wraparound).")

    # Wraparound (% of the ~2.1B read-only wall). ATTENTION only if it's critically high
    # OR something is PINNING xmin (can't self-heal); otherwise WARNING. A healthy DB sits
    # ~9% (riding the autovacuum force-freeze trigger), so neither fires — no false alarm.
    alert_pct = float(_get(V_WRAP_ALERT_PCT, 45))
    crit_pct = float(_get(V_WRAP_CRIT_PCT, 70))
    if worst_wrap >= alert_pct:
        pins = _xmin_pins()
        if worst_wrap >= crit_pct or pins:
            why = (f" Clear what's pinning xmin: {'; '.join(pins)}." if pins
                   else " No xmin pin found — autovacuum is falling behind; check it's running / not starved.")
            attention.append(f"Transaction-ID / MultiXact wraparound at {worst_wrap:.0f}% of the ~2.1B read-only "
                             f"limit.{why}")
        else:
            warning.append(f"Wraparound at {worst_wrap:.0f}% of the ~2.1B read-only limit — nothing is pinning "
                          f"xmin, so autovacuum will bring it down on its own. No action needed; if it climbs "
                          f"past {crit_pct:.0f}%, check with your DBA.")

    # Bloated tables not reindexed: split into intentional skips vs genuine failures.
    reindexed_set = set(reindexed)
    intentionally_skipped = set(skipped_active) | set(skipped_drop_failed)
    pending = [t for t in bloated if t not in reindexed_set and t not in intentionally_skipped]
    if pending and not blocked:
        if _reindex_enabled():
            attention.append(f"{len(pending)} bloated table(s) could NOT be reindexed this run "
                             f"(rebuild failed/interrupted) — check task logs: {', '.join(pending)}.")
        else:
            warning.append(f"{len(pending)} table(s) over {pct:.0f}% index bloat, reindex disabled (report-only) "
                          f"— enable {V_ENABLE_REINDEX} when ready.")

    # Tables where the automatic DROP of the leftover invalid index itself failed -> ATTENTION.
    if skipped_drop_failed:
        attention.append(f"{len(skipped_drop_failed)} bloated table(s) skipped — the automatic "
                         f"DROP INDEX CONCURRENTLY of their leftover _ccnew/_ccold index failed this run; "
                         f"check task logs and drop manually, then reindex will retry automatically: "
                         f"{', '.join(skipped_drop_failed)}.")

    # Tables skipped because an index build is already in flight -> WARNING (no action; retries next run).
    if skipped_active:
        warning.append(f"{len(skipped_active)} bloated table(s) skipped — reindex already in progress "
                       f"(pg_stat_progress_create_index); will retry next daily run: {', '.join(skipped_active)}.")

    # Per-table VACUUM (FREEZE) failure (only reachable when enabled) -> ATTENTION.
    freeze_pending = [t for t in near_wrap if t not in set(frozen)]
    if freeze_pending and _freeze_enabled() and not freeze_blocked:
        attention.append(f"{len(freeze_pending)} table(s) near wraparound could NOT be frozen this run "
                         f"(VACUUM FREEZE failed) — check task logs: {', '.join(freeze_pending)}.")

    # Genuine orphaned invalid indexes (backend no longer running) -> ATTENTION, manual cleanup.
    if invalid_idx:
        attention.append(f"{len(invalid_idx)} orphaned invalid index(es) still present — the PostgreSQL "
                         f"backend that was building them is no longer running. "
                         f"Clear manually with DROP INDEX CONCURRENTLY, then reindex will retry: "
                         f"{', '.join(r['index_name'] for r in invalid_idx)}.")

    # Invalid indexes whose PG backend is still running after our client disconnected -> WARNING, no action.
    # PostgreSQL only detects a client disconnect when it tries to write output; REINDEX CONCURRENTLY
    # never does, so the backend keeps running silently. These are NOT orphans — do not drop them.
    if inprogress_idx:
        tables = sorted({r["table_name"] for r in inprogress_idx})
        warning.append(f"REINDEX still running on PostgreSQL backend for {len(tables)} table(s) "
                       f"(client connection dropped, but PostgreSQL continues silently) — "
                       f"no action needed; indexes will complete or be cleaned automatically next run: "
                       f"{', '.join(tables)}.")

    # Stale stats not analyzed: ATTENTION if enabled+failed, WARNING if disabled.
    stale_pending = [t for t in stale if t not in set(analyzed)]
    if stale_pending:
        if _analyze_enabled():
            attention.append(f"{len(stale_pending)} table(s) with stale planner stats could NOT be analyzed: "
                             f"{', '.join(stale_pending)}.")
        else:
            warning.append(f"{len(stale_pending)} table(s) with stale planner stats, analyze disabled "
                          f"(report-only): {', '.join(stale_pending)}.")

    # Long transactions open -> WARNING (early warning; usually clear on their own).
    if txns:
        warning.append(f"{len(txns)} transaction(s) open > {mins} min — can make reindex wait and pin xmin; "
                      f"see the session list below and clear if stuck.")

    status = "ATTENTION" if attention else ("WARNING" if warning else "HEALTHY")
    context["ti"].xcom_push(key="status", value=status)

    skip_parts = ([f"{len(skipped_drop_failed)} skipped (DROP failed — see ATTENTION)"] if skipped_drop_failed else []) + \
                 ([f"{len(skipped_active)} skipped (already in progress — retries next run)"] if skipped_active else [])
    if reindexed:
        rnote = f"Reindexed {len(reindexed)} table(s) — see the 'reindexed' column below."
        if skip_parts:
            rnote += " " + "; ".join(skip_parts) + "."
    elif blocked:
        rnote = "Skipped (blocked) — see ATTENTION above."
    elif skipped_drop_failed or skipped_active:
        rnote = "; ".join(skip_parts) + "."
    elif bloated:
        if _reindex_enabled():   # enabled but none succeeded -> genuine failure, not report-only
            rnote = f"{len(bloated)} bloated table(s) over {pct:.0f}% could NOT be reindexed — see ATTENTION."
        else:
            rnote = f"{len(bloated)} bloated table(s) over {pct:.0f}%, reindex disabled (report-only)."
    else:
        rnote = f"No table over {pct:.0f}% estimated index bloat — nothing to reindex."
    if dropped_invalid:
        rnote += f" Auto-dropped {len(dropped_invalid)} leftover invalid index(es) before reindex."
    if frozen:
        fnote = (f"Froze {len(frozen)} table(s): {', '.join(frozen)} — wraparound now "
                 f"{worst_wrap:.0f}% of the limit.")
        if freeze_pending:   # some succeeded, some failed — don't silently omit the failures
            fnote += f" {len(freeze_pending)} FAILED (see ATTENTION): {', '.join(freeze_pending)}."
    elif freeze_blocked:
        fnote = "Skipped (blocked) — see ATTENTION above."
    elif near_wrap:
        if _freeze_enabled():   # enabled but none frozen -> genuine failure, not report-only
            fnote = (f"{len(near_wrap)} table(s) near the freeze limit ({', '.join(near_wrap)}) could NOT be "
                     f"frozen — see ATTENTION.")
        else:
            fnote = (f"{len(near_wrap)} table(s) near the freeze limit ({', '.join(near_wrap)}), "
                     f"freeze disabled (report-only).")
    else:
        fnote = f"No table near the freeze limit (all < {warn:.0f}%)."
    if analyzed:
        anote = f"Analyzed {len(analyzed)} table(s): {', '.join(analyzed)}."
    elif stale:
        if _analyze_enabled():   # enabled but none succeeded -> genuine failure, not report-only
            anote = f"{len(stale)} table(s) with stale stats could NOT be analyzed ({', '.join(stale)}) — see ATTENTION."
        else:
            anote = f"{len(stale)} table(s) with stale stats ({', '.join(stale)}), analyze disabled (report-only)."
    else:
        anote = "Planner stats current — nothing to analyze."
    log.info("status=%s; reindex: %s; freeze: %s; analyze: %s", status, rnote, fnote, anote)

    # ---- HTML: 3-level banner (red ATTENTION / amber WARNING / green HEALTHY),
    # showing the action-required list and the for-review list separately. ----
    if attention:
        head = ('<div style="background:#c0392b;color:#fff;padding:10px;font-size:16px;font-weight:bold">'
                '&#9888; ATTENTION NEEDED — action required</div>')
    elif warning:
        head = ('<div style="background:#e67e22;color:#fff;padding:10px;font-size:16px;font-weight:bold">'
                '&#9888; WARNING — review; no immediate action, check with your DBA if it persists</div>')
    else:
        head = ('<div style="background:#27ae60;color:#fff;padding:10px;font-size:16px;font-weight:bold">'
                '&#10003; HEALTHY — no action needed</div>')
    banner = head
    if attention:
        banner += ('<p style="margin:6px 0 0;color:#c0392b"><b>Action required:</b></p><ul>'
                   + "".join(f"<li>{a}</li>" for a in attention) + "</ul>")
    if warning:
        banner += ('<p style="margin:6px 0 0;color:#b9770e"><b>For review (check with your DBA if it persists):</b></p><ul>'
                   + "".join(f"<li>{w}</li>" for w in warning) + "</ul>")

    wrap_html = ('<p><b>Wraparound (% of the ~2.1B read-only limit; healthy is single digits):</b></p>'
                 '<table border="1" cellpadding="4" cellspacing="0">'
                 '<tr><th>database</th><th>XID</th><th>MultiXact</th></tr>'
                 + "".join(f"<tr><td>{w['datname']}</td>"
                           f"<td align=right>{100.0*w['xid_age']/WRAP_HARD_LIMIT:.0f}%</td>"
                           f"<td align=right>{100.0*w['mxid_age']/WRAP_HARD_LIMIT:.0f}%</td></tr>"
                           for w in wrap) + "</table>")

    if txns:
        txn_html = ('<p><b>Long / idle-in-transaction sessions:</b></p>'
                    '<table border="1" cellpadding="4" cellspacing="0">'
                    '<tr><th>pid</th><th>user</th><th>state</th><th>age(min)</th><th>query</th></tr>'
                    + "".join(f"<tr><td>{t['pid']}</td><td>{t['usename']}</td><td>{t['state']}</td>"
                              f"<td align=right>{t['min']}</td><td>{t['query']}</td></tr>" for t in txns)
                    + "</table>")
    else:
        txn_html = "<p><b>Long / idle-in-transaction sessions:</b> none</p>"

    # Invalid indexes section: auto-cleaned this run (green) + any remaining that still need manual action (red).
    # Per PG docs (https://www.postgresql.org/docs/current/sql-reindex.html):
    #   _ccnew = interrupted rebuild; DROP then reindex again.
    #   _ccold = rebuild succeeded but old index couldn't be swapped out; DROP only, no reindex needed.
    # Both are safe to drop once confirmed orphaned (indisvalid=false, not in pg_stat_progress_create_index).
    _idx_action = {
        "ccnew": "rebuild never finished — DROP INDEX CONCURRENTLY, then reindex again",
        "ccold": "rebuild succeeded — safe to DROP INDEX CONCURRENTLY (no re-run needed)",
        "invalid": "invalid index (failed build) — review, then DROP/REINDEX manually",
    }
    if dropped_invalid:
        auto_rows = "".join(
            f'<tr style="background:#e8f5e9"><td>{r["index_name"]}</td><td>{r["table_name"]}</td>'
            f'<td>{r["kind"]}</td><td>Dropped automatically this run</td></tr>'
            for r in dropped_invalid
        )
        auto_idx_html = (
            '<p><b>Auto-cleaned leftover invalid indexes this run</b> '
            '(<a href="https://www.postgresql.org/docs/current/sql-reindex.html">PG docs</a>: '
            '_ccnew = DROP then reindex; _ccold = DROP only, rebuild already succeeded):</p>'
            '<table border="1" cellpadding="4" cellspacing="0">'
            '<tr><th>index</th><th>table</th><th>kind</th><th>action taken</th></tr>'
            + auto_rows + "</table>"
        )
    else:
        auto_idx_html = ""

    # In-progress: PG backend still running after client disconnect — blue/info, no action.
    if inprogress_idx:
        inprog_rows = "".join(
            f'<tr style="background:#e8f0fe"><td>{r["index_name"]}</td><td>{r["table_name"]}</td>'
            f'<td align=right>{_sz(r["index_bytes"])}</td><td>{r["kind"]}</td></tr>'
            for r in inprogress_idx
        )
        inprog_idx_html = (
            '<p><b>Reindex still running on PostgreSQL backend</b> '
            '(client connection dropped, but PG continues — '
            '<a href="https://www.postgresql.org/message-id/3415.1248745744%40sss.pgh.pa.us">PG only detects disconnect on next write</a>; '
            'REINDEX CONCURRENTLY never writes output so the backend runs silently): '
            '<b>do not drop these indexes</b> — they will complete or be cleaned automatically next run:</p>'
            '<table border="1" cellpadding="4" cellspacing="0">'
            '<tr><th>index</th><th>table</th><th>size so far</th><th>kind</th></tr>'
            + inprog_rows + "</table>"
        )
    else:
        inprog_idx_html = ""

    if invalid_idx:
        remaining_rows = "".join(
            f'<tr style="background:#fdecea"><td>{r["index_name"]}</td><td>{r["table_name"]}</td>'
            f'<td align=right>{_sz(r["index_bytes"])}</td><td>{r["kind"]}</td>'
            f'<td>{_idx_action.get(r["kind"], _idx_action["invalid"])}</td></tr>'
            for r in invalid_idx
        )
        remaining_idx_html = (
            '<p><b>Orphaned invalid indexes</b> (backend no longer running — manual cleanup needed):</p>'
            '<table border="1" cellpadding="4" cellspacing="0">'
            '<tr><th>index</th><th>table</th><th>size</th><th>kind</th><th>suggested manual action</th></tr>'
            + remaining_rows + "</table>"
        )
    else:
        suffix = " (all auto-cleaned this run)" if dropped_invalid else ""
        remaining_idx_html = f"<p><b>Orphaned invalid indexes:</b> none{suffix}</p>"
    idx_html = auto_idx_html + inprog_idx_html + remaining_idx_html

    # Row colour keys off the SAME sets that drive the action (which honour the
    # size floor + threshold), NOT the raw estimate — so the table can never
    # contradict the banner:
    #   green  = reindexed / handled this run
    #   red    = over threshold AND not handled  (<=> an ATTENTION line)
    #   plain  = fine
    min_mb = int(_get(V_MIN_INDEX_MB, 100))
    bloated_set = set(bloated)
    hrows = ""
    for r in after:
        name = r["relname"]
        bp_before = before_bloat_map.get(name)      # bloat % captured before the reindex
        bp_after = bloat_map.get(name)              # bloat % now (after the reindex)
        # both only present for indexes >= the size floor; "—" otherwise (estimate
        # is noise at that size). Shown as before -> after so the change is explicit.
        if bp_before is None and bp_after is None:
            bp_txt = "—"
        else:
            _b = "—" if bp_before is None else f"{bp_before:.0f}%"
            _a = "—" if bp_after is None else f"{bp_after:.0f}%"
            bp_txt = f"{_b} &rarr; {_a}"
        rc = reclaimed_map.get(name)
        rc_txt = f"{_sz(rc[0])} &rarr; {_sz(rc[1])}" if rc else "—"
        if name in reindexed_set:                   # handled this run -> green
            style = ' style="background:#e8f5e9"'
        elif name in bloated_set:                   # needs action -> red (matches an ATTENTION line)
            style = ' style="background:#fdecea"'
            bp_txt = f"<b>{bp_txt}</b>"
        else:
            style = ''
        hrows += (f"<tr{style}><td>{name}</td><td align=right>{r['rows']:,}</td>"
                  f"<td align=right>{r['dead_pct']}</td><td align=right>{_sz(r['heap_bytes'])}</td>"
                  f"<td align=right>{_sz(r['index_bytes'])}</td><td align=right>{_sz(r['total_bytes'])}</td>"
                  f"<td align=right>{bp_txt}</td><td align=right>{r['mod_since_analyze']:,}</td>"
                  f"<td align=right>{r['hrs_since_vac']}</td>"
                  f"<td align=right>{r['hrs_since_analyze']}</td><td align=right>{rc_txt}</td></tr>")
    health_html = ('<p><b>Per-table health:</b></p>'
                   '<table border="1" cellpadding="4" cellspacing="0">'
                   '<tr><th>table</th><th>rows</th><th>dead%</th><th>heap</th><th>indexes</th>'
                   '<th>total</th><th>est_bloat% (before&rarr;after)</th><th>mods_since_analyze</th>'
                   '<th>hrs_since_vac</th><th>hrs_since_analyze</th>'
                   '<th>reindexed (idx before&rarr;after)</th></tr>' + hrows + "</table>")
    legend = ('<p style="font-size:12px;color:#777">'
              '<span style="background:#fdecea">&nbsp;red&nbsp;</span> = over the '
              f'{pct:.0f}% bloat threshold, not yet reindexed (action needed) &nbsp;&middot;&nbsp; '
              '<span style="background:#e8f5e9">&nbsp;green&nbsp;</span> = reindexed this run. '
              f'<b>est_bloat% (before&rarr;after)</b> is a catalog estimate (&plusmn;10%) taken before and '
              f'after this run\'s reindex, shown only for indexes &ge; the size floor ({min_mb} MB); smaller '
              'indexes show &ldquo;&mdash;&rdquo; because the estimate is noise at that size (a near-minimum '
              'index sits at its structural floor, so its estimate stays high and reindex cannot lower it).</p>')

    return (f"<html><body><h3>Airflow DB Maintenance — {label}</h3>"
            f"{banner}"
            f"<p><b>Reindex (index bloat):</b> {rnote}<br>"
            f"<b>Freeze (wraparound):</b> {fnote}<br>"
            f"<b>Analyze (stale stats):</b> {anote}</p>"
            f"{wrap_html}{txn_html}{idx_html}{health_html}{legend}"
            f"</body></html>")


with airflow.DAG(
    dag_id="system_airflow_db_maintenance",
    schedule="0 3 * * *",  # DAILY 03:00 UTC — daily monitoring; reindex only when bloated (gated)
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["system_maintenance"],
    doc_md=__doc__,
    default_args={"owner": "system", "depends_on_past": False},
) as dag:
    snapshot_before = rail.PythonOperator(
        task_id="snapshot_before", python_callable=snapshot_before,
        execution_timeout=timedelta(minutes=10), retries=1,
    )
    reindex_if_bloated = rail.PythonOperator(
        task_id="reindex_if_bloated", python_callable=reindex_if_bloated,
        # 7-day timeout + retries=0: never kill a reindex mid-flight (leaves a
        # _ccnew leftover) and never auto-retry (re-runs hours of work). Next
        # daily run re-checks and retries if needed.
        execution_timeout=timedelta(days=7), retries=0,
    )
    freeze_if_old = rail.PythonOperator(
        task_id="freeze_if_old", python_callable=freeze_if_old,
        # Online VACUUM (FREEZE) — no blocking lock, but can run long on a huge
        # table. retries=0 + 7-day timeout: never killed / never auto-re-run.
        execution_timeout=timedelta(days=7), retries=0,
    )
    analyze_if_stale = rail.PythonOperator(
        task_id="analyze_if_stale", python_callable=analyze_if_stale,
        execution_timeout=timedelta(hours=6), retries=0,
    )
    build_report = rail.PythonOperator(
        task_id="build_report", python_callable=build_report,
        execution_timeout=timedelta(minutes=10), retries=1,
    )
    email_report = rail.EmailOperator(
        task_id="email_report", to=ALERT_EMAIL,
        subject=("[DB Maintenance][{{ ti.xcom_pull(task_ids='snapshot_before', key='region_env') or 'unknown' }}]"
                 " {{ ti.xcom_pull(task_ids='build_report', key='status') or 'OK' }} - {{ ds }}"),
        html_content="{{ ti.xcom_pull(task_ids='build_report') | safe }}",
    )
    email_failure = rail.EmailOperator(
        task_id="email_failure", trigger_rule="one_failed", to=ALERT_EMAIL,
        subject="[DB Maintenance][{{ ti.xcom_pull(task_ids='snapshot_before', key='region_env') or 'unknown' }}] FAILED - {{ ds }}",
        html_content=(
            "<h3>Airflow DB Maintenance FAILED</h3>"
            "<p>DAG: {{ dag.dag_id }}<br>Run: {{ run_id }}<br>Execution: {{ ts }}</p>"
            "<p>A task failed — check task logs:<br>"
            "{{ conf.get('webserver','base_url') }}/dags/{{ dag.dag_id }}/grid</p>"
        ),
    )

    # Sequential: one online maintenance action at a time (reindex -> freeze ->
    # analyze), then the consolidated report/email. Any task failing fires email_failure.
    (snapshot_before >> reindex_if_bloated >> freeze_if_old >> analyze_if_stale
     >> build_report >> email_report)
    [snapshot_before, reindex_if_bloated, freeze_if_old, analyze_if_stale,
     build_report] >> email_failure
