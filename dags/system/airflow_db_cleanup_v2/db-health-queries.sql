-- =============================================================================
-- Airflow Metadata DB — HEALTH / DIAGNOSTIC QUERY PACK
-- =============================================================================
-- Purpose: gather the numbers a DBA needs to (a) understand current bloat/health
-- and (b) design thresholds for an automated maintenance DAG. ALL read-only —
-- nothing here modifies data. Safe to run on production any time.
--
-- CAPTURE ALL RESULTS IN ONE FILE AND UPLOAD IT:
--   psql "host=... dbname=airflow-production user=..." \
--        -f db-health-queries.sql > db-health-results.txt 2>&1
--
-- Some queries need optional extensions (pgstattuple, pg_stat_statements). If an
-- extension isn't installed the query errors harmlessly — leave the error in the
-- output, it just tells us the extension is absent.
--
-- Each section says WHAT TO LOOK FOR so the results are interpretable.
-- =============================================================================

\pset pager off
\timing on

-- =============================================================================
-- SECTION 1 — SIZES: where the space actually is
-- =============================================================================
-- LOOK FOR: total size, and heap vs index split. A table whose index size dwarfs
-- its heap is an index-bloat suspect (e.g. task_instance).
SELECT
    relname,
    pg_size_pretty(pg_table_size(relid))          AS heap_plus_toast,
    pg_size_pretty(pg_indexes_size(relid))         AS indexes,
    pg_size_pretty(pg_total_relation_size(relid))  AS total,
    n_live_tup,
    n_dead_tup
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;


-- =============================================================================
-- SECTION 2 — PER-INDEX SIZES on the big tables  ★ most important for reindex ★
-- =============================================================================
-- LOOK FOR: exactly WHICH indexes on task_instance/xcom/log are huge, and their
-- idx_scan (usage). A huge index with idx_scan=0 is both bloated AND unused.
SELECT
    t.relname                                       AS table_name,
    ix.indexrelid::regclass                         AS index_name,
    pg_size_pretty(pg_relation_size(ix.indexrelid)) AS index_size,
    s.idx_scan                                      AS times_used,
    ix.indisunique                                  AS is_unique,
    ix.indisprimary                                 AS is_primary
FROM pg_index ix
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_stat_user_indexes s ON s.indexrelid = ix.indexrelid
WHERE t.relname IN ('task_instance','xcom','log','dag_run','job',
                    'rendered_task_instance_fields','serialized_dag')
ORDER BY pg_relation_size(ix.indexrelid) DESC;


-- =============================================================================
-- SECTION 3 — DEAD TUPLES & AUTOVACUUM EFFECTIVENESS
-- =============================================================================
-- LOOK FOR: high dead_pct (>20%) or old last_autovacuum on hot tables = autovacuum
-- falling behind. n_mod_since_analyze high = stale planner stats.
SELECT
    relname,
    n_live_tup,
    n_dead_tup,
    round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 0), 1) AS dead_pct,
    n_mod_since_analyze,
    last_autovacuum,
    last_autoanalyze,
    autovacuum_count,
    analyze_count
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC;


-- =============================================================================
-- SECTION 4 — TRANSACTION-ID WRAPAROUND  ★ critical at your churn/volume ★
-- =============================================================================
-- LOOK FOR: pct_to_wraparound climbing toward 100. Above ~60% means autovacuum
-- freeze isn't keeping up and a forced anti-wraparound vacuum is warranted.

-- 4a. Per-table freeze age
SELECT
    c.relname,
    age(c.relfrozenxid)                                        AS xid_age,
    current_setting('autovacuum_freeze_max_age')::bigint       AS freeze_max_age,
    round(100.0 * age(c.relfrozenxid)
          / current_setting('autovacuum_freeze_max_age')::bigint, 1) AS pct_to_wraparound
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 't', 'm')
  AND n.nspname = 'public'
ORDER BY age(c.relfrozenxid) DESC;

-- 4b. Database-level
SELECT datname, age(datfrozenxid) AS db_xid_age
FROM pg_database
ORDER BY age(datfrozenxid) DESC;


-- =============================================================================
-- SECTION 5 — INDEX USAGE: unused & rarely-used indexes
-- =============================================================================
-- LOOK FOR: idx_scan = 0 on non-unique indexes = dead weight (write overhead +
-- space + reindex cost for nothing) = candidates to DROP (human decision).
SELECT
    s.relname                                      AS table_name,
    s.indexrelname                                 AS index_name,
    s.idx_scan                                     AS times_used,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    ix.indisunique                                 AS is_unique
FROM pg_stat_user_indexes s
JOIN pg_index ix ON ix.indexrelid = s.indexrelid
WHERE NOT ix.indisprimary
ORDER BY s.idx_scan ASC, pg_relation_size(s.indexrelid) DESC;


-- =============================================================================
-- SECTION 6 — DUPLICATE / REDUNDANT INDEXES
-- =============================================================================
-- LOOK FOR: rows returned = indexes covering the identical columns = one is
-- redundant and can be dropped (human decision).
SELECT
    indrelid::regclass AS table_name,
    array_agg(indexrelid::regclass) AS duplicate_indexes,
    pg_size_pretty(sum(pg_relation_size(indexrelid))) AS combined_size
FROM pg_index
GROUP BY indrelid, indkey, indclass, indexprs, indpred
HAVING count(*) > 1
ORDER BY sum(pg_relation_size(indexrelid)) DESC;


-- =============================================================================
-- SECTION 7 — SEQ-SCAN HOTSPOTS (possible missing indexes)
-- =============================================================================
-- LOOK FOR: large tables with high seq_scan relative to idx_scan = queries doing
-- full scans = possible missing index (investigate, don't auto-add).
SELECT
    relname,
    seq_scan,
    seq_tup_read,
    idx_scan,
    n_live_tup,
    CASE WHEN seq_scan > 0
         THEN round(seq_tup_read::numeric / seq_scan, 0) END AS avg_rows_per_seqscan
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC
LIMIT 20;


-- =============================================================================
-- SECTION 8 — LIVE ACTIVITY: long / idle-in-transaction / blocking
-- =============================================================================
-- These block autovacuum and are a ROOT CAUSE of bloat. Snapshot is momentary —
-- run a few times across the day if possible.

-- 8a. Active / long-running / idle-in-transaction sessions
SELECT
    pid,
    usename,
    state,
    now() - xact_start   AS xact_age,
    now() - query_start  AS query_age,
    wait_event_type,
    wait_event,
    left(query, 120)     AS query
FROM pg_stat_activity
WHERE state <> 'idle'
ORDER BY xact_start NULLS LAST;

-- 8b. Who is blocking whom
SELECT
    pid,
    pg_blocking_pids(pid) AS blocked_by,
    now() - xact_start    AS xact_age,
    left(query, 120)      AS query
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0;

-- 8c. Any vacuum currently running (and its progress)
SELECT p.pid, c.relname, p.phase,
       p.heap_blks_total, p.heap_blks_scanned, p.heap_blks_vacuumed
FROM pg_stat_progress_vacuum p
JOIN pg_class c ON c.oid = p.relid;


-- =============================================================================
-- SECTION 9 — CACHE HIT RATIOS (memory pressure)
-- =============================================================================
-- LOOK FOR: ratios below ~0.99 on a busy OLTP metadata DB can indicate
-- shared_buffers / RAM pressure worsened by bloat.
SELECT
    round(sum(heap_blks_hit)   / nullif(sum(heap_blks_hit + heap_blks_read), 0), 4) AS heap_cache_hit_ratio,
    round(sum(idx_blks_hit)    / nullif(sum(idx_blks_hit  + idx_blks_read), 0), 4)  AS index_cache_hit_ratio
FROM pg_statio_user_tables;


-- =============================================================================
-- SECTION 10 — CURRENT AUTOVACUUM CONFIG (global + per-table overrides)
-- =============================================================================
-- LOOK FOR: whether the one-time per-table tuning has been applied yet, and
-- whether global thresholds are the defaults (scale_factor 0.2 = too lax here).

-- 10a. Global settings
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
    'autovacuum', 'autovacuum_max_workers', 'autovacuum_naptime',
    'autovacuum_vacuum_scale_factor', 'autovacuum_analyze_scale_factor',
    'autovacuum_vacuum_threshold', 'autovacuum_vacuum_cost_limit',
    'autovacuum_vacuum_cost_delay', 'autovacuum_freeze_max_age',
    'maintenance_work_mem'
)
ORDER BY name;

-- 10b. Per-table storage overrides already set (empty = none applied yet)
SELECT relname, reloptions
FROM pg_class
WHERE reloptions IS NOT NULL
ORDER BY relname;


-- =============================================================================
-- SECTION 11 — TOP QUERIES BY TIME (needs pg_stat_statements)
-- =============================================================================
-- LOOK FOR: whether task_instance/dag_run/job index access dominates total time
-- = confirms scheduler bottleneck. Errors here just mean the extension is absent.
SELECT
    left(query, 120)  AS query,
    calls,
    round(total_exec_time)        AS total_ms,
    round(mean_exec_time, 2)      AS mean_ms,
    rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 25;


-- =============================================================================
-- SECTION 12 — ACCURATE BLOAT via pgstattuple (needs pgstattuple extension)
-- =============================================================================
-- pgstattuple gives EXACT bloat but SCANS the object — expensive on big tables.
-- The _approx variant samples and is much cheaper. If the extension is missing,
-- these error harmlessly; Sections 1-3 still give us a strong bloat picture.
--
-- Enable (one-time, if you have rights):  CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- 12a. Approx table bloat (cheap, sampled) — free space % ~= reclaimable bloat.
SELECT 'task_instance' AS tbl, * FROM pgstattuple_approx('task_instance');
SELECT 'dag_run'       AS tbl, * FROM pgstattuple_approx('dag_run');
SELECT 'job'           AS tbl, * FROM pgstattuple_approx('job');
SELECT 'xcom'          AS tbl, * FROM pgstattuple_approx('xcom');

-- 12b. Index bloat via pgstatindex — run for the biggest indexes flagged in
--      Section 2. Replace the index name below and repeat as needed.
--      avg_leaf_density well under ~70-80% indicates a bloated index.
-- SELECT * FROM pgstatindex('task_instance_pkey');
-- SELECT * FROM pgstatindex('<index_name_from_section_2>');

-- =============================================================================
-- END — upload db-health-results.txt
-- =============================================================================
