# QA Testing Guide — Ensure Project Tasks (JIT)

**Scope:** `resource_planner_ensure_project_tasks_*`
**Direction:** Polaris → Resource Planner (event-triggered, fire-and-forget)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0
**Common mechanics:** see [qa_testing_reference.md](qa_testing_reference.md)

---

## 0. Context

| | Ensure Project Tasks (JIT) |
|---|---|
| What it does | When an allocation references a project/task not yet synced to `rp_source_time_codes`, this DAG (triggered fire-and-forget by the allocation DAGs) checks presence and pulls from Polaris if needed — so orphan rows resolve within seconds. |
| Triggered by | `task_resource_allocation_export/child_dag.py`, `task_resource_allocation_export_webhooks/main_added.py`, `..._main_updated_child_processor.py` (all fire one trigger per unique project per page) |
| Polaris source | `POST /services/TaskListService1.svc/GetData` (paginated, pagesize 1000) |
| RP read | `POST /api/v1/rp/sourceTimeCodesProjectTasks/checkPresence` |
| RP write | `PUT /api/v1/rp/sourceTimeCodesProjectTasks` (MERGE) |
| Target table | `dbo.rp_source_time_codes` |
| Schedule | **None** — only triggered |
| Concurrency | `max_active_runs=20` |
| Idempotency | MERGE — safe to retry / fire concurrently |

### Flow

```
view_dag_run_conf
   │
capture_conf
   │
prepare_check_payload → check_presence (POST /checkPresence)
   │
has_missing? ─No→ end_task                  (~1 second, no Polaris call)
   │ Yes
fetch_from_polaris (TaskListService, paged)
   │
prepare_upsert_payload (canonical format — must match project_task_export_delta)
   │
upsert_time_codes (PUT /sourceTimeCodesProjectTasks)
   │
end_task
```

Plus `log_sync_failure` on `one_failed` trigger rule.

### Per-row contract (must match `project_task_export_delta`)

| Column | Project row | Task row |
|---|---|---|
| `type` | `'project'` | `'task'` |
| `task_level` | `0` | depth of `fullPathItems` |
| `time_code` | `project_id` | `'{project_id}~{task_id}'` |
| `parent_time_code` | `project_id` | `project_id` |
| `time_code_name` | `project_name` | `'{project_name}~{ancestor1}~...~{leaf}'` |
| `parent_time_code_name` | `project_name` | `project_name` |
| `project_manager_id` | `""` (JIT can't resolve PM) | `""` |
| `time_entry_enabled` | `0` | `1` |
| `actual_time_code` | NULL | bare `task_id` |
| `actual_time_code_name` | NULL | leaf task name |
| `predecessor_time_code` | NULL | `project_id` for level-1, else parent task's bare id |

---

## 1. Pre-requisites

1. **DAG unpaused** — `resource_planner_ensure_project_tasks_dev` (and `_dev2`).
2. Airflow Variable `rp_tenant_id_dev` set (required to build `project_uri` for the TaskListService call).
3. Gateway endpoint `/sourceTimeCodesProjectTasks/checkPresence` exists (added recently; verify with `curl -k -X POST .../checkPresence -d '{...}'`).
4. Polaris reachable.

---

## 2. Test Cases

### TC-EPT-01 · Happy path: brand-new project + tasks

**How to test**
1. **Polaris**: create a new project + tasks (don't run `project_task_export_delta` afterwards).
2. **Airflow**: manually trigger `resource_planner_ensure_project_tasks_dev` with conf:
   ```json
   {
     "project_id":   "<NEW_PROJECT_ID>",
     "task_ids":     ["<NEW_TASK_ID_1>", "<NEW_TASK_ID_2>"],
     "sourceSystem": "Polaris"
   }
   ```

**Expected**
- `check_presence` returns `{"projectPresent": false, "missingTaskIds": ["...","..."]}`.
- `has_missing` → Yes.
- `fetch_from_polaris` paged TaskListService call returns all tasks for the project.
- `prepare_upsert_payload` builds 1 project row + N task rows in canonical format.
- `upsert_time_codes` PUTs (insertCount > 0, updateCount = 0).

**Verify**
```sql
SELECT type, task_level, time_code, time_code_name,
       actual_time_code, predecessor_time_code
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<NEW_PROJECT_ID>'
 ORDER BY type DESC, task_level;
```
Expect rows in the canonical format. Compare against TC-PTD-01 expected output — should match exactly.

---

### TC-EPT-02 · Happy path: nothing missing → fast no-op

**How to test**
1. TC-EPT-01 complete.
2. Re-trigger the same conf.

**Expected**
- `check_presence` returns `{"projectPresent": true, "missingTaskIds": []}`.
- `has_missing` → No.
- `fetch_from_polaris` is **skipped** (pink in UI).
- `prepare_upsert_payload` skipped.
- `upsert_time_codes` skipped.
- DAG completes in **< 2 seconds**.

This is the steady-state happy path. Most JIT runs in production hit this branch.

---

### TC-EPT-03 · Partial — project present, some tasks missing

**How to test**
1. Run `project_task_export_delta` for a project (so the project row exists in RP).
2. In Polaris, add a new task to that project.
3. Trigger JIT with conf including the new task in `task_ids`.

**Expected**
- `check_presence` returns `{"projectPresent": true, "missingTaskIds": ["<NEW_TASK_ID>"]}`.
- JIT fetches all tasks for the project (even existing ones — full project refresh).
- MERGE inserts the new task row; updates existing rows (no-op updates).

---

### TC-EPT-04 · MERGE doesn't create duplicates vs project_task_export_delta

**Goal:** verify the format alignment. This is the critical regression test.

**How to test**
1. Run `project_task_export_delta` for a project — rows exist in canonical format.
2. Trigger JIT for the **same project** (same conf as if it were brand new).
3. JIT will see the project as present (TC-EPT-02 path) — but force the fetch by:
   - **Option A**: Delete the project row in RP. JIT sees `projectPresent=false`, fetches, re-inserts.
   - **Option B**: Modify the test so `check_presence` always reports missing (e.g. mock the endpoint).

**Expected**
- After JIT runs, no duplicate `(source_system, time_code, type)` rows.

**Verify**
```sql
SELECT time_code, type, COUNT(*) AS n
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<TEST_PROJECT_ID>'
 GROUP BY time_code, type
HAVING COUNT(*) > 1;
```
Expect **0 rows**.

---

### TC-EPT-05 · Hierarchy: nested tasks get correct `taskLevel` + `predecessor_time_code`

**How to test**
1. **Polaris**: create a project with:
   - Top-level task `Design`
   - Nested under Design: `Wireframes` (level 2)
   - Nested under Wireframes: `Iconography` (level 3)
2. Trigger JIT with conf including all 3 task IDs.

**Expected**
- After JIT runs, the 3 task rows have:

| time_code | task_level | predecessor_time_code |
|---|---|---|
| `<PID>~Design` | 1 | `<PID>` |
| `<PID>~Wireframes` | 2 | `<Design's task_id>` (bare) |
| `<PID>~Iconography` | 3 | `<Wireframes' task_id>` (bare) |

And `time_code_name = "Project Name~Design~Wireframes~Iconography"` for the deepest one.

---

### TC-EPT-06 · Pagination — project with > 1000 tasks

**How to test**
1. (Hard to engineer; skip in dev unless you have such a project.) If you can: a project with > 1000 tasks.
2. Trigger JIT.

**Expected**
- `task_page_handler` advances `page` and re-fetches while the response returns ≥ pagesize rows.
- `task_result_handler` flattens all pages into one list.
- All tasks land in `rp_source_time_codes`.

---

### TC-EPT-07 · Tenant ID Variable missing

**How to test**
1. Delete `rp_tenant_id_dev`.
2. Trigger JIT.

**Expected**
- `get_task_list_payload` raises `KeyError` (Variable not found).
- DAG fails.
- `log_sync_failure` POSTs a failure record.

**Cleanup**: restore the variable.

---

### TC-EPT-08 · Polaris empty response (project has no tasks)

**How to test**
1. **Polaris**: create a project with NO tasks.
2. Trigger JIT.

**Expected**
- TaskListService returns `rows: []`.
- `task_result_handler` returns `[]`.
- `prepare_upsert_payload` builds **only the project row** (no task rows).
- Upsert succeeds — 1 row inserted.

**Verify**
```sql
SELECT type, COUNT(*) FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris' AND parent_time_code = '<EMPTY_PROJECT_ID>'
 GROUP BY type;
```
Expect: 1 row with `type='project'`, 0 with `type='task'`.

---

### TC-EPT-09 · Concurrent fires for the same project — both succeed (idempotent MERGE)

**How to test**
1. Trigger 2 JIT runs back-to-back for the **same** project (within 1 second).

**Expected**
- Both runs complete successfully.
- DB has exactly one project row + N task rows (no duplicates).
- One run inserts; the second run updates (no-op MERGE).

---

### TC-EPT-10 · Trigger from allocation DAG carries `masterRunId`

**Goal:** confirm the conf propagated by the allocation DAG includes `masterRunId` for correlation.

**How to test**
1. Trigger the allocation bulk DAG (which fires JIT triggers as part of its flow).
2. In the JIT DAG runs list, find one that was triggered by the allocation DAG.
3. Click the run → `view_dag_run_conf` task → log.

**Expected**
- Conf JSON contains `masterRunId: "<allocation-DAG-run-id>"` and `triggeredByDagId: "resource_planner_task_resource_allocation_export_child_dev"` (or whichever parent).

---

### TC-EPT-11 · Failure logging on Polaris timeout

**How to test**
1. (Simulated) block the Replicon connection.
2. Trigger JIT.

**Expected**
- `fetch_from_polaris` times out / fails.
- `log_sync_failure` runs (trigger_rule=one_failed) → POSTs to `/confirmedBookings/failures`.

**Verify**
```sql
SELECT * FROM dbo.rp_export_sync_failures
 WHERE child_dag_id LIKE '%ensure_project_tasks%'
   AND status = 'PENDING_RETRY'
 ORDER BY first_failed_at DESC;
```

(Note: the `/confirmedBookings/failures` table is shared with the confirmed_bookings_export — JIT failures land there too. Filter by `child_dag_id LIKE '%ensure_project_tasks%'`.)

---

## 3. Cross-DAG / regression

### TC-EPT-XGEN-01 · After JIT run, project_task_export_delta is a no-op

**Goal:** once JIT writes canonical rows, the next delta cycle finds them present and skips them.

**How to test**
1. TC-EPT-01 done.
2. Run `project_task_export_delta_dev`.
3. Watch its insert/update counts.

**Expected**
- For the JIT-written project, delta finds the rows already present.
- MERGE update_count = N (every row touched but no values changed).
- insert_count for that project = 0.

---

### TC-EPT-XGEN-02 · Triggers from all 3 allocation DAGs work

Verify each of these does fire the JIT in real conditions:
- `task_resource_allocation_export_child_*` (bulk)
- `task_resource_allocation_export_webhooks/main_added`
- `task_resource_allocation_export_webhooks/main_updated_child_processor`

Pattern: trigger each parent, check JIT runs list, confirm a new JIT run appeared with the right conf.

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY
DELETE FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code IN ('<TEST_PROJECT_IDS>');

DELETE FROM dbo.rp_export_sync_failures
 WHERE child_dag_id LIKE '%ensure_project_tasks%'
   AND last_attempted_at > '<test_start_time>';
```

---

## 5. Sign-off criteria

- [ ] TC-EPT-01 (happy path, new project) — pass
- [ ] TC-EPT-02 (no-op skip path) — pass (**critical** — most production runs hit this branch)
- [ ] TC-EPT-03 (partial — project present, task missing) — pass
- [ ] **TC-EPT-04 (no duplicates vs delta)** — pass (**critical** — prevents the format-drift bug)
- [ ] TC-EPT-05 (hierarchy: levels & predecessor) — pass
- [ ] TC-EPT-08 (empty project) — pass
- [ ] TC-EPT-09 (concurrent fires) — pass
- [ ] TC-EPT-10 (`masterRunId` propagated) — pass
- [ ] TC-EPT-XGEN-01 (delta sees JIT rows as up-to-date) — pass (**critical**)
- [ ] Steady-state run completes in **< 2 seconds** (TC-EPT-02 timing)

---

## 6. Known limitations / out of scope

- **`project_manager_id` is empty** in JIT-written rows (the JIT doesn't run the user report). The next `project_task_export_delta` cycle backfills it. Acceptable for the JIT's purpose (allocation rows don't need PM ID).
- **Bulk back-pressure**: a massive allocation push that triggers JIT for 100 projects in a short window will queue runs past `max_active_runs=20`. Steady state catches up in minutes. Out of scope to tune.
- **Polaris empty-cellCollection responses**: if a project has zero tasks but Polaris returns no rows, `task_result_handler` returns `[]` — only the project row is written. If Polaris later adds tasks, the next allocation against them re-triggers JIT.
