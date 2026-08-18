-- =============================================================================
-- Scheduler-health runbook for the Airflow metadata DB (all regions)
-- =============================================================================
-- WHEN TO USE: DAGs are scheduling slowly / not on time, scheduler lag is high.
-- The scheduler lives on TWO tables: task_instance and dag_run. When those get
-- stale stats or bloated indexes, every scheduler query slows and the loop lags.
--
-- Do the steps IN ORDER. Stop as soon as the scheduler recovers.
-- All steps are ONLINE / no downtime.
-- Root cause reference:
--   stale planner stats  -> fixed by Step 2 (VACUUM ANALYZE)
--   bloated indexes      -> fixed by Step 3 (REINDEX CONCURRENTLY)
--   dead-tuple buildup   -> prevented by Step 1 (autovacuum tuning)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1 — Autovacuum tuning (one-time, persistent, instant, metadata-only)
-- -----------------------------------------------------------------------------
-- Keeps dead tuples low and (critically) keeps the scheduler's planner stats
-- fresh so it doesn't drift into bad plans. task_instance + dag_run analyze more
-- aggressively (0.005) because they are the scheduler-critical tables.
ALTER TABLE task_instance SET (autovacuum_vacuum_scale_factor=0.01, autovacuum_analyze_scale_factor=0.005);
ALTER TABLE dag_run       SET (autovacuum_vacuum_scale_factor=0.01, autovacuum_analyze_scale_factor=0.005);
ALTER TABLE job           SET (autovacuum_vacuum_scale_factor=0.02, autovacuum_analyze_scale_factor=0.01);
ALTER TABLE xcom          SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);


-- -----------------------------------------------------------------------------
-- STEP 2 — Refresh stats (do this FIRST when the scheduler is slow)
-- -----------------------------------------------------------------------------
-- Cheap, instant, online. Stale stats make the planner pick bad plans for the
-- scheduler's queries. This alone often fixes scheduler lag. If it recovers,
-- STOP HERE — you don't need Step 3.
VACUUM (ANALYZE) task_instance;
VACUUM (ANALYZE) dag_run;


-- -----------------------------------------------------------------------------
-- STEP 3 — Rebuild bloated indexes (only if Step 2 did NOT fix it)
-- -----------------------------------------------------------------------------
-- If the scheduler is still slow after fresh stats, the cause is bloated indexes
-- on task_instance/dag_run (huge index files no longer fit in cache -> slow scans
-- even with a correct plan). VACUUM ANALYZE cannot shrink indexes; only REINDEX.
--
-- ONLINE / no downtime (CONCURRENTLY). task_instance takes HOURS (its ti_state_lkp
-- index is hundreds of GB) — the scheduler keeps running throughout.
--
-- RULES:
--   * Run one table at a time; let each FINISH.
--   * DO NOT CANCEL — a cancelled reindex leaves an invalid "<index>_ccnew"
--     leftover. If that happens: DROP INDEX CONCURRENTLY <name>_ccnew; then re-run.
--   * Make sure no long/idle transaction is open first (CONCURRENTLY waits on it):
--       SELECT pid, state, now()-xact_start AS age FROM pg_stat_activity
--       WHERE xact_start IS NOT NULL AND now()-xact_start > interval '5 min';
--   * Watch progress from another session:
--       SELECT now()-query_start AS running, left(query,60)
--       FROM pg_stat_activity WHERE query ILIKE 'REINDEX%';

SET lock_timeout='30s';
REINDEX TABLE CONCURRENTLY task_instance;   -- rebuilds all its indexes (hours, online)
REINDEX TABLE CONCURRENTLY dag_run;         -- rebuilds all its indexes (quick)

-- After Step 3, the scheduler's indexes are compact again. If it STILL lags after
-- fresh stats + reindex, the cause is not the DB — look at Airflow side
-- (scheduler parse time, pools / max_active_runs, executor slots).
-- =============================================================================
