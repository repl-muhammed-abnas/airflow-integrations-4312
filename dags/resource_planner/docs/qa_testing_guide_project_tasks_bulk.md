# QA Testing Guide — Project Tasks Bulk Export

**Scope:** `resource_planner_project_task_export_bulk_*` (master + per-batch child)
**Direction:** Polaris → Resource Planner (forward, manual / periodic full snapshot)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0
**Common mechanics:** see [qa_testing_reference.md](qa_testing_reference.md)

---

## 0. Context

| | Project Tasks Bulk export |
|---|---|
| What it does | Pulls **the full project + task catalogue** from Polaris (no delta filter), batches it, and UPSERTs into RP. Used for full reconciliation after a clock skew, source data drift, or first-time deployment. |
| Polaris source | Same task report + TaskListService + user report as the delta DAG — but without the "changed since" filter |
| RP write | `PUT /api/v1/rp/sourceTimeCodesProjectTasks` (MERGE) |
| Target table | `dbo.rp_source_time_codes` |
| Schedule | None (manual / on-demand) |
| Sibling DAG | `project_task_export_delta_*` runs hourly with a "changed since" filter; this one is the catch-up |
| Idempotency | MERGE-based — safe to re-run any number of times |

### When QA runs the bulk vs the delta

| Scenario | Use |
|---|---|
| Routine hourly testing of a recent change | Delta |
| First-time tenant setup | Bulk |
| Suspected drift between Polaris and RP | Bulk (re-baseline) |
| Polaris report had a delta-filter bug or skipped projects | Bulk |
| Validating row format on day one | Bulk (smaller surface, no delete-child) |

---

## 1. Pre-requisites

See [§1 / §4 of the reference](qa_testing_reference.md).

| Item | Notes |
|---|---|
| Variable `resource_planner_project_task_export_bulk_enable_batch_task_{instance}` | Set to `"false"` |
| `rp_tenant_id_{instance}` | Required |
| Polaris user report (PM lookup) | Must exist and be named per `config.user_report_name` |
| Polaris task report (full catalogue) | Must exist per `config.task_report_name` |

**Baseline:** same query as project_task_export_delta:
```sql
SELECT
    SUM(CASE WHEN type='project' THEN 1 ELSE 0 END) AS projects,
    SUM(CASE WHEN type='task'    THEN 1 ELSE 0 END) AS tasks
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris';
```

---

## 2. Test Cases

### TC-PTB-01 · First-time full population

**Goal:** confirm the DAG can populate an empty table from scratch.

**How to test**
1. **DEV ONLY:** truncate the test rows you control:
   ```sql
   DELETE FROM dbo.rp_source_time_codes
    WHERE source_system = 'Polaris'
      AND parent_time_code IN ('<test_project_ids>');
   ```
2. Confirm Variable `..._enable_batch_task_dev = "false"`.
3. Trigger DAG `resource_planner_project_task_export_bulk_dev`.

**Expected**
- Master pulls **the full project list**, batches it (`config.batch_size`).
- Master triggers N children, one per batch.
- Each child upserts. Insert counts > 0; update counts = 0 (on the first run).

**Verify**
```sql
SELECT COUNT(*) AS rows_added FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris';
```
Should be ≈ (number of projects in Polaris) + (number of tasks in Polaris).

---

### TC-PTB-02 · Re-run idempotency (MERGE updates, doesn't insert)

**How to test**
1. TC-PTB-01 done.
2. Re-trigger the DAG immediately.

**Expected**
- Insert count ≈ 0, update count = total row count (every row got "touched" but no values changed).
- DB row count unchanged.

**Verify**
```sql
SELECT COUNT(*) FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris';
```
Same as after TC-PTB-01.

---

### TC-PTB-03 · Row-format compatibility with delta DAG

**Goal:** bulk + delta must write rows in the **same format** so MERGE works across both writers.

**How to test**
1. Run bulk (TC-PTB-01).
2. Pick one project. Note: `time_code`, `type`, `actual_time_code`, `predecessor_time_code` for its rows.
3. Trigger the delta DAG (`project_task_export_delta_dev`).
4. Re-check the same rows.

**Expected**
- Bulk and delta rows are byte-identical for the same project's data.
- No duplicates created by the second writer.

**Verify** with the TC-PTD-08 duplicate-detection query (see project_tasks_delta guide).

---

### TC-PTB-04 · Large project (> 100 tasks)

**Goal:** confirm batching handles a project with a large number of tasks without timing out.

**How to test**
1. **Polaris:** find or create a project with 100+ tasks.
2. Trigger the bulk DAG.

**Expected**
- The child processing that batch completes in reasonable time (< 5 min for ~500 tasks).
- TaskListService side-call paginates correctly (no skipped tasks).

**Verify**
```sql
SELECT COUNT(*) FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code = '<LARGE_PROJECT_ID>'
   AND type = 'task';
```
Should equal Polaris's task count for that project.

---

### TC-PTB-05 · Skip path: batch toggle ON

Set variable to `"true"` → DAG short-circuits via `batch_task` → no children triggered, no inserts. See TC-USR-03 for pattern.

---

### TC-PTB-06 · No master schedule (manual only)

Confirm `schedule_interval` resolves to `None` in `instances/dev.py` (otherwise it'd run as a duplicate of the delta DAG). If a schedule is set by mistake, the DAG would create wasteful hourly snapshots.

**How to test**
- DAG list view → bulk DAG → look at the "Schedule" column. Should show **None** or **@once**, NOT a cron expression.

---

### TC-PTB-07 · Gateway failure mid-run

**Goal:** confirm partial completion when the gateway fails between batches.

**How to test**
1. While master is running and has fired 2 of N children, take down the gateway.
2. Watch:
   - Currently-running children may fail with `ConnectionError`.
   - Master continues triggering remaining children (they'll also fail).
   - Master completes (`all_done` triggers it forward) even with failed children.

**Expected**
- Some children fail.
- Restore gateway.
- Trigger the bulk DAG again — MERGE handles the rest cleanly (no duplicates, fills in the gap).

---

## 3. Cross-DAG / regression

### TC-PTB-XGEN-01 · After bulk run, delta produces 0 inserts

Run bulk, immediately run delta. The delta should see "everything's up-to-date" and skip / no-op.

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY
DELETE FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND parent_time_code IN ('<TEST_PROJECT_IDS>');
```

---

## 5. Sign-off criteria

- [ ] TC-PTB-01 (full population) — pass
- [ ] TC-PTB-02 (idempotency) — pass
- [ ] TC-PTB-03 (delta-compat row format) — pass (**critical**)
- [ ] TC-PTB-04 (large project) — pass
- [ ] TC-PTB-06 (no automatic schedule) — pass
- [ ] No `WARN`/`ERROR` lines that aren't expected

---

## 6. Known limitations / out of scope

- **Project deletions**: bulk does not delete rows that no longer exist in Polaris. Only the delta DAG's delete-child handles that. If a project was deleted in Polaris, bulk leaves the stale rows in RP — call delta to clean them, or do it manually.
- **Concurrency with delta**: don't run bulk and delta simultaneously. Both write to the same target with MERGE; while it's safe, you can end up with confusing "update count" interpretations. Run one at a time.
- **Large tenants (10k+ projects)**: bulk run time grows linearly with project count. Out of scope for QA timing — covered by load testing separately.
