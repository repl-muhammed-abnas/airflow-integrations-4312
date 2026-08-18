# Airflow Metadata DB — Health Check Results (formatted)

Formatted results of `db-health-queries.sql` run against `airflow-production`.
Raw psql capture: `db-health-queries-result.sql`.

**Headline:** ~1.5 TB DB. Bloat is concentrated in 3 `task_instance` indexes
(475 GB). Root cause = a 2 h idle-in-transaction blocking autovacuum/freeze; DB is
at 98 % of XID freeze age. `pg_stat_statements` and `pgstattuple` are NOT installed.

---

## §1 — Sizes (top tables)

| Table | Heap+TOAST | Indexes | Total | Live rows | Dead |
|---|---|---|---|---|---|
| `task_instance` | 92 GB | **514 GB** | 606 GB | 209.3 M | 7.6 M |
| `log` | 283 GB | 76 GB | 360 GB | 795.2 M | 0 |
| `log_backup` | 185 GB | 42 GB | 228 GB | 0 | 0 |
| `xcom` | 146 GB | 72 GB | 218 GB | 133.3 M | 0.19 M |
| `dag_run` | 45 GB | 24 GB | 69 GB | 13.1 M | 0.97 M |
| `job` | 12 GB | 25 GB | 37 GB | 52.5 M | 1.2 M |
| `rendered_task_instance_fields` | 9.9 GB | 175 MB | 10 GB | 337 K | 29 K |
| `serialized_dag` | 127 MB | 928 KB | 128 MB | 3 K | 17.7 K |

`task_instance` indexes (514 GB) dwarf its 92 GB heap → severe index bloat.

---

## §2 — Per-index sizes (the reindex targets)

| Table | Index | Size | Times used | Note |
|---|---|---|---|---|
| task_instance | `ti_state_lkp` | **284 GB** | 650 M | hot, ~50× bloated → **REINDEX #1** |
| task_instance | `task_instance_pkey` | **121 GB** | 385 | required, bloated → **REINDEX** |
| task_instance | `ti_state_incl_start_date` | **70 GB** | 143 M | hot, bloated → **REINDEX** |
| xcom | `idx_xcom_task_instance` | 50 GB | 868 M | hot |
| log | `log_pkey1` | 26 GB | 633 M | hot |
| log | `idx_log_dttm` | 26 GB | 173 | used by cleanup DELETE — keep |
| xcom | `xcom_pkey` | 20 GB | 795 M | hot |
| task_instance | `ti_dag_run` | 15 GB | 1.7 B | hot |
| job | `job_type_heart` | 14 GB | 28 M | hot |
| log | `log_dag_id_idx` | 14 GB | 242 | rarely used |
| log | `log_event_idx` | 12 GB | 4.8 K | rarely used |
| dag_run | `idx_last_scheduling_decision` | 9.4 GB | **0** | **UNUSED → drop** |
| job | `idx_job_dag_id` | 2.6 GB | **2** | **~unused → drop** |

**3 indexes = 475 of the 514 GB** of task_instance bloat.

---

## §3 — Dead tuples & autovacuum

Big tables are healthy (low dead %); autovacuum keeps up on deletes. Bloat is
**historical index bloat that VACUUM cannot shrink — only REINDEX reclaims it.**

| Table | Dead % | `autovacuum_count` | Note |
|---|---|---|---|
| `task_instance` | 3.5 % | 39 | healthy |
| `job` | 2.2 % | 25 | healthy |
| `dag_run` | 6.9 % | 239 | healthy |
| `xcom` | 0.1 % | 22 | healthy |
| `log` | 0 % | 14 | healthy |
| `dag` | 89 % | **33 318** | tiny table, autovacuum thrashing |
| `serialized_dag` | 85 % | **25 596** | tiny, thrashing |
| `trigger` | 98 % | **11 871** | tiny, thrashing |

Small-table thrash + only **3 autovacuum workers** → consider more workers.

---

## §4 — Transaction-ID wraparound  ⚠️

`autovacuum_freeze_max_age = 200 M`.

| Database | XID age | % of limit |
|---|---|---|
| `airflow-pre-production` | 199.9 M | **99.96 %** |
| `airflow-production` | 196.5 M | **98.2 %** |
| `airflow-qa` | 194.4 M | 97.2 % |
| `template1` | 170.6 M | 85.3 % |

Right at the forced anti-wraparound threshold — driven by the §8 blocker.

---

## §5 — Unused indexes (drop candidates, active tables)

| Table | Index | Size | Scans |
|---|---|---|---|
| `dag_run` | `idx_last_scheduling_decision` | 9.4 GB | **0** |
| `job` | `idx_job_dag_id` | 2.6 GB | **2** |
| `log` | `log_dag_id_idx` | 14 GB | 242 |
| `log` | `log_event_idx` | 12 GB | 4.8 K |

(`log_backup` indexes excluded — orphan table.)

---

## §7 — Seq-scan hotspots

Mostly the **cleanup DAG's own** full-scan queries + tiny tables Postgres
deliberately seq-scans. `dag_run` 17.7 K full scans (74 B tuples) and
`task_instance` 4.7 K full scans stand out — largely the pre-fix cleanup logic.

---

## §8 — Live activity  ⚠️ ROOT CAUSE

| PID | DB | State | Age | Query |
|---|---|---|---|---|
| 21660 | pre-production | **idle in transaction** | **2 h 02 m** | `ANALYZE job` |
| 24278 | pre-production | active (Lock) | 1 h 47 m | `ANALYZE job` (blocked by 21660) |

A 2-hour idle-in-transaction holds back the xmin horizon → autovacuum can't clean
or freeze → **this is what created the bloat and pinned the freeze age at 98 %.**

---

## §10 — Autovacuum config

| Setting | Value | Note |
|---|---|---|
| `autovacuum` | on | |
| `autovacuum_vacuum_scale_factor` | 0.1 | better than default; still lax for 200 M-row tables |
| `autovacuum_analyze_scale_factor` | 0.05 | |
| `autovacuum_freeze_max_age` | 200 M | default |
| `autovacuum_max_workers` | **3** | low for this DB size |
| `autovacuum_vacuum_cost_delay` | 5 ms | |
| `autovacuum_vacuum_cost_limit` | 2400 | fairly aggressive |
| `maintenance_work_mem` | ~1.1 GB | |
| **Per-table overrides** | **none** | tuning not yet applied |

---

## §11 / §12 — Extensions

- `pg_stat_statements` — **not installed** (no query-level timing available).
- `pgstattuple` — **not installed** (no exact bloat %; sizes/usage used instead).

---

## Actions this drove (see `plaform-issues/db-one-time-fixes.sql` + the maintenance DAG)
1. Kill idle-in-transaction (§8) — unblocks autovacuum/freeze.
2. `VACUUM (FREEZE)` big tables (§4) — controlled, before Postgres forces it.
3. `REINDEX` `ti_state_lkp`, `task_instance_pkey`, `ti_state_incl_start_date` (§2).
4. `DROP` `idx_last_scheduling_decision`, `idx_job_dag_id` (§5).
5. Per-table autovacuum tuning (§10). Consider more autovacuum workers (§3).
