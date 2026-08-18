# QA Testing Guide — Task Resource Allocation Export (Bulk)

**Scope:** `resource_planner_task_resource_allocation_export_*` (master + N per-project children)
**Direction:** Polaris → Resource Planner (forward, schedule/manual, full project scan)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0
**Common mechanics:** see [qa_testing_reference.md](qa_testing_reference.md)

---

## 0. Context

| | Task Resource Allocation — Bulk |
|---|---|
| What it does | For each project in scope, pulls all (user × task) allocations via Polaris GraphQL, expands schedule rules into daily rows, **hash-compares against the previous run's reference snapshot**, and writes inserts/deletes to RP. |
| Polaris source | User report + Task report + GraphQL `taskResourceUserAllocationsForUser` |
| SFTP source | Previous run's hash-reference CSV (used for delta detection across runs) |
| RP write — insert | `PUT /api/v1/rp/sourceAllocations` (replacement payload — gateway deletes old + inserts new per booking) |
| RP write — delete | `PATCH /api/v1/rp/sourceAllocations` (marks `hours=0`) |
| Side trigger ⭐ | `trigger_ensure_project_tasks` (fire-and-forget per project — JIT fills `rp_source_time_codes` gaps) |
| Target table | `dbo.rp_source` (non–Absence/Holiday rows; `outbound_pending_op` left NULL) |
| Schedule | Configurable (None in dev / manual) |
| Concurrency | Master `max_active_runs=1`; per-project child `max_active_runs=10` (configurable) |

### DAG topology

```
master:                                       per-project children:
  task_resource_allocation_export_dev   ─►    ..._child_dev_{1,2,3,...} (max_active_runs=10)
                                              │
                                              └─ fires ensure_project_tasks (fire-and-forget)
```

### Critical flow inside the child

```
fetch_allocations (GraphQL, paged)
   │
expand_schedule_rules (daily rows)
   │
extract_project_task_pairs ──► trigger_ensure_project_tasks (fire-and-forget, no wait)
   │
detect_deltas (SHA256 hash per allocation_id, compared to SFTP reference)
   │
has_changes ──Yes──► prepare_api_payload ──► write_to_database (PUT) ──► mark_deleted_allocations (PATCH) ──► publish_hashes
              └─No──► publish_hashes (record current hashes for next run)
```

### Per-row contract in `rp_source`

| Column | Value |
|---|---|
| `source_booking_id` | **bare allocation UUID** (no `_00001` suffix — that was removed) |
| `source_system` | `'Polaris'` |
| `time_code` | `'{project_id}~{task_id}'` |
| `users_user_id` | from user report → resource map |
| `hours` | per-day from schedule rules |
| `work_date` | per-day |
| `hours_type` | derived: `'Client Project'` / `'Internal Non-Billable'` (NOT Absence/Holiday) |
| `labor_code` | from labor code map |
| `employee_id` | from user report |
| `outbound_pending_op` | **NULL** (this is a Polaris → RP write; only confirmed_bookings_export sets the outbound signal) |

---

## 1. Pre-requisites

| Item | Notes |
|---|---|
| Variable `resource_planner_task_resource_allocation_export_enable_batch_task_{instance}` | `"false"` |
| `rp_tenant_id_{instance}` | Required |
| SFTP credentials (reference file storage) | `config.sftp_conn_id` must be set up — fetches previous-run hash CSV |
| Polaris reports | "Resource Planner Project - User Template" + a Task report (per config) |
| User has at least one allocation in Polaris | Otherwise the master DAG short-circuits |
| `ensure_project_tasks_{instance}` DAG | **Must be unpaused** — the child fires it; if paused, no harm but the JIT no-op'd |

---

## 2. Test Cases

### TC-TRA-01 · Happy path: new allocation lands as N daily rows

**How to test**
1. **Polaris:** open a project, find a task, find a user. Allocate that user to that task for **5 working days, 8h each**.
2. **Airflow:** trigger `resource_planner_task_resource_allocation_export_dev`.

**Expected**
- Master pulls user + task reports → builds a per-project child trigger list.
- One child runs for that project.
- Child: GraphQL returns 1 allocation. `expand_schedule_rules` yields 5 rows. `detect_deltas` finds them as new → all 5 go to `to_insert`.
- `write_to_database` returns 200. `publish_hashes` records the hash for next-run comparison.

**Verify**
```sql
SELECT source_booking_id, work_date, hours, time_code, hours_type
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id = '<ALLOCATION_UUID>'
 ORDER BY work_date;
```
Expect **5 rows**, all sharing the same `source_booking_id`, distinct `work_date`s, `hours = 8.0`, `time_code = "{project_id}~{task_id}"`.

---

### TC-TRA-02 · No change → no writes

**How to test**
1. TC-TRA-01 complete. Reference hashes have been written.
2. Without changing anything in Polaris, re-trigger the DAG.

**Expected**
- `detect_deltas` finds: current hashes == previous reference → `to_insert=[], to_delete=[]`.
- `has_changes` → No → goes straight to `publish_hashes`.
- No DB writes.

---

### TC-TRA-03 · Modify an allocation: hours change

**How to test**
1. TC-TRA-01 done.
2. **Polaris:** change one day's hours from 8 → 6.

**Expected**
- `detect_deltas` SHA256 differs from reference. Allocation goes to BOTH `to_delete` (mark old rows hours=0) AND `to_insert` (new 5 rows).
- `prepare_api_payload` builds:
  - `replacements: [{sourceBookingIdPrefix: <UUID>, records: [new 5 rows]}]`
  - `mark_deleted: []` (allocation still exists, just modified)
- Gateway's `replacements` PUT: deletes old rows + inserts new ones in one transaction.

**Verify**
```sql
SELECT work_date, hours FROM dbo.rp_source
 WHERE source_booking_id = '<UUID>'
 ORDER BY work_date;
```
Modified day shows `hours = 6`, others still `8`.

---

### TC-TRA-04 · Allocation removed in Polaris → marked deleted in RP

**How to test**
1. TC-TRA-01 done.
2. **Polaris:** delete the allocation.
3. Trigger the DAG.

**Expected**
- `detect_deltas`: allocation present in reference but missing in current GraphQL → goes to `to_delete`.
- `prepare_api_payload`:
  - `replacements: []`
  - `mark_deleted: [{sourceBookingIdPrefix: <UUID>}]`
- `mark_deleted_allocations` PATCH fires → gateway sets `hours = 0` for all rows where `source_booking_id LIKE '<UUID>%'`.

**Verify**
```sql
SELECT source_booking_id, work_date, hours FROM dbo.rp_source
 WHERE source_booking_id = '<UUID>';
```
Expect rows still there with `hours = 0`. (Or zero rows if your gateway does hard-delete on `markDeleted` — verify against backend code.)

---

### TC-TRA-05 · JIT trigger fires when a brand-new project's allocations come in

**Goal:** when an allocation references a project not yet in `rp_source_time_codes`, the JIT (`ensure_project_tasks`) should fire and fill the gap.

**Why this matters:** this is the orphan-prevention that the JIT was built for.

**How to test**
1. **Polaris:** create a brand-new project + task, allocate a user to it.
2. **Before** running `project_task_export_delta`, trigger the allocation bulk DAG.
3. Watch in Airflow:
   - The child DAG runs through `extract_project_task_pairs` → `trigger_ensure_project_tasks`.
   - In the Browse → DAG Runs view, look for a fresh run of `resource_planner_ensure_project_tasks_dev`.

**Expected**
- One `ensure_project_tasks` run is triggered per project on the page.
- Its conf shows: `{"project_id": "<NEW_PROJECT_ID>", "task_ids": ["<NEW_TASK_ID>"], "masterRunId": "<allocation-DAG-run-id>"}`.
- The JIT calls `/checkPresence`, sees the project is missing, fetches via TaskListService, upserts.
- Within ~30 seconds, the project + task rows appear in `rp_source_time_codes`.

**Verify**
```sql
SELECT type, time_code, time_code_name FROM dbo.rp_source_time_codes
 WHERE source_system='Polaris' AND parent_time_code = '<NEW_PROJECT_ID>';
```
Expect at least 1 project row + 1 task row (more if Polaris had additional tasks under the project).

---

### TC-TRA-06 · JIT doesn't re-fetch when project already exists

**Goal:** if `rp_source_time_codes` already has the project + task, the JIT short-circuits.

**How to test**
1. TC-TRA-05 done. Project rows exist.
2. Add another allocation against the same project + task.
3. Trigger the bulk DAG.

**Expected**
- A new `ensure_project_tasks` run fires.
- `check_presence` returns `projectPresent=true, missingTaskIds=[]`.
- `has_missing` → **No** → end_task. ~1 second total. **No TaskListService call** to Polaris.

**Verify** by inspecting the JIT DAG run's logs: `fetch_from_polaris` task should be **skipped** (pink).

---

### TC-TRA-07 · Multi-project run: master correctly fans out children

**How to test**
1. **Polaris:** ensure allocations exist across **3 different projects**.
2. Trigger the master DAG.

**Expected**
- Master triggers 3 children (one per project).
- All run in parallel up to `max_active_runs_child=10`.
- Each writes its own allocations independently.

**Verify**
```sql
SELECT DISTINCT LEFT(time_code, CHARINDEX('~', time_code) - 1) AS project_id, COUNT(*) AS rows
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND last_updated_date > '<test_start_time>'
   AND hours_type NOT IN ('Absence', 'Holiday')
 GROUP BY LEFT(time_code, CHARINDEX('~', time_code) - 1);
```
Expect 3 distinct projects, each with its own allocations.

---

### TC-TRA-08 · No allocations → DAG completes cleanly

**How to test**
1. **Polaris:** pick a project that has no allocations.
2. Trigger the child directly (or wait for it to run via master).

**Expected**
- `fetch_allocations` returns empty.
- `expand_schedule_rules` yields 0 rows.
- `detect_deltas`: nothing to insert, nothing to delete.
- DAG completes successfully, no writes.

---

### TC-TRA-09 · SHA256 reference file recovery from SFTP

**Goal:** confirm the SFTP reference file (previous run's hashes) is correctly loaded.

**How to test**
1. After TC-TRA-01, check the SFTP `config.sftp_reference_base_path` directory.
2. A file named `config.sftp_reference_file` should exist with the hashes from the last run.
3. Open it; it should contain CSV with `allocation_id`, `hash` columns (per the schema in code).

**Optional injection test:** corrupt the file (rename or truncate) and re-run. The DAG should:
- `has_previous_reference` → No → builds empty dict → treats all current allocations as new → re-inserts everything.

---

### TC-TRA-10 · Schedule fires (if scheduled)

If the master is scheduled (check `config.schedule_interval` in `instances/{instance}.py`), confirm it fires per the cron and runs with `max_active_runs=1` (queue subsequent runs if a prior one is still working).

---

### TC-TRA-11 · Skip path: batch toggle ON

Standard pattern. See TC-USR-03.

---

### TC-TRA-12 · Allocation with > 1000 days in one schedule rule

**Goal:** confirm `expand_schedule_rules` handles long date ranges without OOM.

**How to test**
1. **Polaris:** create an allocation spanning a date range that, after expansion, produces ~365+ days.
2. Trigger the DAG.

**Expected**
- `expand_schedule_rules` produces all rows (one per workday respecting weekday exclusions).
- DAG completes; rows land in `rp_source`.

---

## 3. Cross-DAG / regression

### TC-TRA-XGEN-01 · No orphan allocations after a run

After a bulk run + the JIT fire-and-forget DAGs complete (give them ~30s), there should be **zero** allocations pointing at missing time-codes:

```sql
SELECT a.source_booking_id, a.time_code
  FROM dbo.rp_source a
  LEFT JOIN dbo.rp_source_time_codes t
    ON t.source_system = a.source_system
   AND t.time_code = a.time_code
   AND t.type = 'task'
 WHERE a.source_system = 'Polaris'
   AND a.hours_type NOT IN ('Absence', 'Holiday')
   AND a.last_updated_date > '<test_start_time>'
   AND t.time_code IS NULL;
```

Expect **0 rows**. If you see orphans, either:
- The JIT didn't fire (check `ensure_project_tasks` DAG state)
- The JIT fired but failed (check its run logs)
- The project_task hierarchy in Polaris is unusual and the JIT couldn't resolve it (check TaskListService response)

---

### TC-TRA-XGEN-02 · No `outbound_pending_op` pollution

This DAG writes Polaris → RP. It should **never** set `outbound_pending_op` on rows it writes.

```sql
SELECT COUNT(*) FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND outbound_pending_op IS NOT NULL
   AND hours_type NOT IN ('Absence', 'Holiday')
   AND last_updated_date > '<test_start_time>';
```
Expect **0**. Only confirmed_bookings_export–pending rows should carry this signal.

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY — kill the allocations from this test
DELETE FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id IN ('<TEST_UUID_1>', '<TEST_UUID_2>');

-- And the JIT-written time-codes
DELETE FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<TEST_PROJECT_ID>';
```

On SFTP, optionally remove the reference file to force a "from scratch" run next time:
```bash
sftp <user>@<host>
rm <config.sftp_reference_path>
```

In Polaris, delete or archive test allocations + project.

---

## 5. Sign-off criteria

- [ ] TC-TRA-01 (happy path, multi-day) — pass
- [ ] TC-TRA-02 (idempotency) — pass
- [ ] TC-TRA-03 (modify → replace) — pass
- [ ] TC-TRA-04 (delete → mark) — pass
- [ ] **TC-TRA-05 (JIT fires for new project)** — pass (**critical** — orphan prevention)
- [ ] TC-TRA-06 (JIT skips when present) — pass
- [ ] TC-TRA-07 (multi-project fan-out) — pass
- [ ] TC-TRA-XGEN-01 (no orphans) — pass (**critical**)
- [ ] TC-TRA-XGEN-02 (no outbound signal pollution) — pass
- [ ] No `WARN`/`ERROR` lines unexpected

---

## 6. Known limitations / out of scope

- **Allocation owner change** (user A → user B on same task): current DAG sees this as delete-of-A + new-allocation-of-B. The DB ends up with both — old hours=0 rows and new full-hours rows. Documented; might be confusing in reports.
- **Hash collision risk**: SHA256 is effectively collision-free for these payloads. Out of scope to test.
- **JIT failure → orphan window**: if `ensure_project_tasks` fails (Polaris down at JIT time), the allocation row stays orphaned until the next master run's JIT trigger retries. The bulk run itself doesn't block on JIT.
- **Concurrent runs**: master has `max_active_runs=1`; child is 10. If many projects change at once, runtime grows linearly past the concurrency cap.
- **Children running BatchTaskRunOperator**: the JIT trigger task is serialized into the chain so BatchTaskRunOperator's linear-chain assumption holds; tested via the test grid.
