# QA Testing Guide — Project Tasks Delta Export

**Scope:** `resource_planner_project_task_export_delta_*` (master + per-batch child + delete-child DAGs)
**Direction:** Polaris → Resource Planner (forward, schedule-driven pull)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0
**Common mechanics:** see [qa_testing_reference.md](qa_testing_reference.md)

---

## 0. Context

| | Project Tasks Delta export |
|---|---|
| What it does | Pulls **only the projects that have changed since the last run** from Polaris, computes upserts and deletions, and writes to RP. Three DAGs work together. |
| Polaris sources | Project URI report (lists changed projects) + per-project Task report (full hierarchy) + TaskListService1.svc/GetData (time-entry-enabled flags) + User report (for project manager → ID mapping) |
| RP read | Existing rows from `rp_source_time_codes` for delta comparison |
| RP write — upsert | `PUT /api/v1/rp/sourceTimeCodesProjectTasks` (MERGE) |
| RP write — delete | `DELETE /api/v1/rp/sourceTimeCodesProjectTasks` (3 modes: specific, projectCascade, byProjectName) |
| Target table | `dbo.rp_source_time_codes` (with `type='project'` or `type='task'`) |
| Schedule | `0 * * * *` (hourly, top of hour) |
| Idempotency | MERGE-based — same row coming through twice is updated in-place, no duplicates |

### DAG topology

```
master:                                    children:                         delete-child:
  project_task_export_delta_dev    ─►      ..._child_dev (per project batch)
                                                                            ..._delete_child_dev
```

| DAG ID | Role |
|---|---|
| `resource_planner_project_task_export_delta_{instance}` | Master — pulls the list of changed/deleted projects, splits into batches, fans out |
| `resource_planner_project_task_export_delta_child_{instance}` | Processes one batch — pulls full hierarchy for those projects, upserts |
| `resource_planner_project_task_export_delta_delete_child_{instance}` | Processes deletions (projects gone from Polaris) |

### Per-row contract written to `rp_source_time_codes`

| Column | Project row | Task row |
|---|---|---|
| `type` | `'project'` | `'task'` |
| `task_level` | `0` | `1, 2, 3, ...` (hierarchy depth) |
| `time_code` | `project_id` (bare) | `'{project_id}~{task_id}'` (composite!) |
| `parent_time_code` | `project_id` | `project_id` |
| `time_code_name` | `project_name` | `'{project_name}~{ancestor1}~...~{leaf}'` |
| `parent_time_code_name` | `project_name` | `project_name` |
| `project_manager_id` | resolved user_id or `""` | same as project's |
| `time_entry_enabled` | `0` | `1` only if Polaris flag is true |
| `actual_time_code` | `NULL` | `task_id` (bare, last URN segment) |
| `actual_time_code_name` | `NULL` | task leaf name |
| `predecessor_time_code` | `NULL` | `project_id` for level-1, else parent task's bare id |

**Critical:** this format **must match** what `ensure_project_tasks` writes (the JIT resolver). The MERGE natural key is `(source_system, time_code, type)`. If either writer drifts, you'll get phantom duplicates with mixed case/format.

---

## 1. Pre-requisites

See [qa_testing_reference.md §1](qa_testing_reference.md#1-tools--access-you-need-before-starting). Specific to this DAG:

1. **Airflow Variable**: `resource_planner_project_task_export_enable_batch_task_dev = "false"` ([§4](qa_testing_reference.md#4-setting-airflow-variables)).
2. **Polaris reports must exist** with these names (or whatever `config.project_uri_for_delta_report_name`, `user_report_name`, and the per-batch task-report name resolve to):
   - "Project URI for Delta" (or similar — lists changed/deleted projects)
   - "Resource Planner Project - User Template" (for PM name → user_id)
   - A task report (consumed by the child)
3. **Tenant ID variable**: `rp_tenant_id_dev` (used for URI construction).
4. **DB baseline** ([§6.2](qa_testing_reference.md#62-common-verification-queries)):
   ```sql
   SELECT
       SUM(CASE WHEN type='project' THEN 1 ELSE 0 END) AS projects,
       SUM(CASE WHEN type='task'    THEN 1 ELSE 0 END) AS tasks
     FROM dbo.rp_source_time_codes
    WHERE source_system = 'Polaris';
   ```

---

## 2. Test Cases

### TC-PTD-01 · Happy path: add a brand-new project with tasks

**Goal:** confirm a new Polaris project lands as 1 project row + N task rows.

**How to test**
1. **In Polaris** (UI):
   - Navigate to: `Administration > Projects > New Project`.
   - Create a project named **`QA-Test-Project-{your initials}`**, code `QATEST01`.
   - Add 3 top-level tasks: `Design`, `Build`, `Test`. Mark each as "Allow time entry".
   - Note the **project ID** (from the URL: `.../project/<UUID-or-id>`).
   - Note the **task IDs**.
2. **In Polaris** make sure the project shows up in the "Project URI for Delta" report (it should — newly created projects are in the delta).
3. **In Airflow**:
   - Confirm `resource_planner_project_task_export_enable_batch_task_dev = "false"`.
   - Trigger DAG `resource_planner_project_task_export_delta_dev`.
4. Wait for master + child DAG runs to complete.

**Expected**
- Master DAG: branches `Yes` on the has-changed-projects path, triggers the child for the new project's batch.
- Child DAG (`..._delta_child_dev`): pulls the task report for that batch, runs `identify_records_to_upsert`, returns 4 records (1 project + 3 tasks), calls `upsert_records` (HTTP 200).

**Verify**
```sql
SELECT type, task_level, time_code, time_code_name,
       actual_time_code, actual_time_code_name, predecessor_time_code
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<YOUR_PROJECT_ID>'
 ORDER BY type DESC, task_level, time_code;
```
Expect **4 rows**:

| type | task_level | time_code | time_code_name | actual_time_code | predecessor_time_code |
|---|---|---|---|---|---|
| project | 0 | `<PID>` | `QA-Test-Project-XX` | NULL | NULL |
| task | 1 | `<PID>~<TID1>` | `QA-Test-Project-XX~Design` | `<TID1>` | `<PID>` |
| task | 1 | `<PID>~<TID2>` | `QA-Test-Project-XX~Build` | `<TID2>` | `<PID>` |
| task | 1 | `<PID>~<TID3>` | `QA-Test-Project-XX~Test` | `<TID3>` | `<PID>` |

---

### TC-PTD-02 · Idempotency: nothing changed in Polaris between two runs

**Goal:** confirm the second hourly tick is a no-op when nothing changed.

**Setup:** TC-PTD-01 complete.

**How to test**
1. Make sure the "Project URI for Delta" report uses a since-last-run filter (this is the report's job).
2. Trigger the master DAG again.

**Expected**
- Master DAG: `get_project_uri_report` returns 0 changed projects → master DAG short-circuits → no children triggered.
- Or: children are triggered with empty batches → upsert MERGE finds no rows to insert or update → log shows `0 inserts, 0 updates`.

**Verify**
- DB row count unchanged.
- No new rows in `rp_source_time_codes`.

---

### TC-PTD-03 · Project name change → time_code_name updates in-place

**Goal:** confirm a renamed project doesn't create a duplicate row; the existing row is updated.

**How to test**
1. **In Polaris**: rename the test project to `QA-Test-Project-{your initials}-RENAMED`.
2. The project should re-appear in the delta report (it's been "modified").
3. **In Airflow**: trigger the master DAG.

**Expected**
- Child runs `identify_records_to_upsert`.
- Inside the gateway, the MERGE finds matching `(source_system, time_code, type)` rows and **updates** them.
- Insert count = 0, update count > 0.

**Verify**
```sql
SELECT time_code_name
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND time_code = '<YOUR_PROJECT_ID>'
   AND type = 'project';
```
Expect the new name.

```sql
-- And: no duplicates
SELECT time_code, type, COUNT(*)
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<YOUR_PROJECT_ID>'
 GROUP BY time_code, type
HAVING COUNT(*) > 1;
```
Expect **0 rows** (no dups).

---

### TC-PTD-04 · Nested tasks: hierarchy is preserved with proper `taskLevel` and `predecessor_time_code`

**Goal:** confirm 2-level (and deeper) task hierarchies write the right level and predecessor.

**How to test**
1. **In Polaris**: under your test project, add a nested task:
   - Parent: `Design`
   - Child: `Wireframes` (under `Design`)
2. Trigger the master DAG (after letting Polaris register the change).

**Expected**
- 1 new task row: `Wireframes` with `task_level=2`, `predecessor_time_code = <Design's task_id>` (the bare id, not the URN).

**Verify**
```sql
SELECT time_code, time_code_name, task_level, predecessor_time_code, actual_time_code
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<YOUR_PROJECT_ID>'
   AND type = 'task'
 ORDER BY task_level, time_code;
```
The `Wireframes` row should have:
- `task_level = 2`
- `time_code_name = QA-Test-...~Design~Wireframes`
- `predecessor_time_code = <Design's bare task_id>` (NOT the project's id)
- `actual_time_code = <Wireframes' bare task_id>`

---

### TC-PTD-05 · Time-entry-enabled flag

**Goal:** confirm the TaskListService call correctly sets `time_entry_enabled` per task.

**How to test**
1. **In Polaris**: edit one task in your test project, set it to **"Allow time entry: No"**. Leave the others as Yes.
2. Trigger the master DAG.

**Expected**
- Child task `fetch_time_entry_enabled_tasks` runs and returns the URIs of tasks where the flag is true.
- For each task row, `time_entry_enabled` is set accordingly.

**Verify**
```sql
SELECT time_code_name, time_entry_enabled
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<YOUR_PROJECT_ID>'
   AND type = 'task';
```
Tasks with Polaris "Allow time entry = No" should show `time_entry_enabled = 0`; others `1`.

---

### TC-PTD-06 · Delete path: project removed in Polaris → rows removed in RP

**Goal:** confirm the delete-child DAG cleans up rows for projects that were deleted in Polaris.

**How to test**
1. **In Polaris**: delete (or archive) your test project so that the delete-detection report picks it up.
2. Trigger the master DAG.
3. Master triggers the delete-child for that project.

**Expected**
- `delete_child` DAG runs.
- One of three modes fires depending on the source:
  - **specific** — exact (source_system, time_code, type) match (used when the delete report gives a specific task)
  - **projectCascade** — `DELETE WHERE parent_time_code = <project_id>` (used when the whole project is deleted with URI known)
  - **byProjectName** — `DELETE WHERE parent_time_code_name = <name>` (used when only the name is known)

**Verify**
```sql
SELECT COUNT(*)
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<YOUR_PROJECT_ID>';
```
Expect **0 rows** (all project + tasks gone).

---

### TC-PTD-07 · Batch boundaries — multiple changed projects in one master run

**Goal:** confirm the master correctly batches and fans out N children when many projects changed at once.

**How to test**
1. **In Polaris**: rename 3 different test projects within a 5-minute window (so they all appear in the next delta report).
2. Trigger the master DAG.

**Expected**
- Master pulls the URI list → `len = 3`.
- Master batches them (see `config.batch_size`, e.g. 50) and triggers `len/batch_size` children.
- For a small dev tenant, this is typically 1 child run with 3 projects in the batch.

**Verify**
- All 3 projects have their `time_code_name` updated to the new name.

---

### TC-PTD-08 · MERGE conflict-resolution: row added by JIT, then refreshed by delta

**Goal:** confirm that a row inserted by `ensure_project_tasks` (JIT) is correctly **updated** (not duplicated) by the next delta run.

**Why this matters:** both writers use the same `(source_system, time_code, type)` key. If they disagree on format (capitalization, time_code shape), you get phantom duplicates.

**How to test**
1. **In Polaris**: create a project + task. **Don't** wait for the delta to run.
2. Push an allocation against it (use the bulk allocation export DAG or a webhook). This triggers `ensure_project_tasks` (JIT) which inserts the project + task row(s) into `rp_source_time_codes`.
3. **Now trigger** `project_task_export_delta_dev`.

**Expected**
- The MERGE detects the JIT row and **updates** it (no insert).
- Final row count for that project: 1 project + N tasks (no duplicates).

**Verify**
```sql
SELECT type, time_code, COUNT(*)
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<YOUR_PROJECT_ID>'
 GROUP BY type, time_code
HAVING COUNT(*) > 1;
```
Expect **0 rows**. If you see anything here, the JIT and delta are writing in different formats — file a bug.

---

### TC-PTD-09 · Skip path: batch task toggle ON

Same as TC-USR-03 in the users guide, applied here. Set the variable to `"true"`, trigger master, confirm `batch_task` runs and no children are triggered.

---

### TC-PTD-10 · Schedule fires hourly

See [qa_testing_reference.md §9](qa_testing_reference.md#9-schedules-in-dev-current). Confirm a `scheduled__<datetime>` run fires at `0 * * * *`.

---

### TC-PTD-11 · Gateway connection failure

See [qa_testing_reference.md §10](qa_testing_reference.md#10-when-something-fails--first-3-checks). Disable the gateway, trigger the DAG, expect master or child to fail with a `ConnectionError`.

---

## 3. Cross-DAG / regression checks

### TC-PTD-XGEN-01 · No orphan allocations after a delta run

**Goal:** the delta DAG must not leave allocation rows pointing at non-existent task rows.

```sql
SELECT a.source_booking_id, a.time_code
  FROM dbo.rp_source a
  LEFT JOIN dbo.rp_source_time_codes t
    ON t.source_system = a.source_system
   AND t.time_code = a.time_code
   AND t.type = 'task'
 WHERE a.source_system = 'Polaris'
   AND a.hours_type NOT IN ('Absence', 'Holiday')
   AND t.time_code IS NULL;
```

Expect **0 rows** after a master+delete-child run completes. If orphans exist, two paths are out of sync — either the delete-child deleted too aggressively, or the JIT didn't catch up. Open a bug with the specific orphan rows attached.

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY
DELETE FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code IN ('<TEST_PROJECT_ID_1>', '<TEST_PROJECT_ID_2>');
```

In Polaris, delete your test project (or archive it) when QA is done.

---

## 5. Sign-off criteria

- [ ] TC-PTD-01 (happy path) — pass
- [ ] TC-PTD-02 (idempotency) — pass
- [ ] TC-PTD-03 (rename → update, not insert) — pass
- [ ] TC-PTD-04 (nested hierarchy) — pass
- [ ] TC-PTD-05 (time-entry-enabled flag) — pass
- [ ] TC-PTD-06 (delete path) — pass
- [ ] TC-PTD-08 (JIT + delta no-duplicates) — pass (**critical** — protects the integration boundary)
- [ ] TC-PTD-XGEN-01 (no orphans) — pass
- [ ] Master DAG duration < 5 min for a typical hourly delta
- [ ] No `WARN`/`ERROR` lines in logs that aren't expected

---

## 6. Known limitations / out of scope

- **Project moved between departments**: this DAG syncs the time-code tree; the project_manager mapping comes from the user report. If the PM changes, you'll see an update; the lookup happens fresh each run.
- **Tasks renamed mid-day**: handled. The MERGE updates `time_code_name`. Old name is overwritten.
- **Projects with > 1000 tasks**: the TaskListService call pages (pagesize=250 by default). Very large projects work but are slower. Out of scope for QA timing checks.
- **Cross-tenant URN collisions**: not testable in dev — each tenant has its own URI prefix.
- **The `taskLevel` calculation**: limited to "what the task report says" — if Polaris's full-path string is inconsistent for some tasks, the level computation can be off-by-one. Spot-check with TC-PTD-04 and a couple of deeper nestings if available.
