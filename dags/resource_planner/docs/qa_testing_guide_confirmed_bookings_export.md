# QA Testing Guide — Confirmed Bookings Export (RP → Polaris)

**Scope:** `resource_planner_confirmed_bookings_export_*` (master) + 3 page-children + 2 op-DAGs (create / update) + sync-failure retry
**Direction:** **Resource Planner → Polaris** (reverse, the only reverse flow)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0
**Common mechanics:** see [qa_testing_reference.md](qa_testing_reference.md)

---

## 0. Context

| | Confirmed Bookings Export |
|---|---|
| What it does | Reads RP rows where `outbound_pending_op IS NOT NULL` (= someone marked them "needs push to Polaris") and writes them as Polaris allocations via GraphQL. Per-day signal: ADD / UPDATE / DELETE. |
| Source | `dbo.rp_source` rows with `outbound_pending_op` set + `last_updated_date` ∈ (cursor, upperBound] |
| Polaris write | GraphQL mutations: `createTaskResourceUserAllocation`, `updateTaskResourceUserAllocation` (PARTIAL), `putTaskResourceEstimate` |
| RP write — markPushed | `PATCH /api/v1/rp/confirmedBookings/markPushed` — clears `outbound_pending_op` after a successful Polaris push |
| Cursor | Airflow Variable `rp_confirmed_bookings_cursor_{instance}` — the last successful upperBound |
| Tenant ID | Airflow Variable `rp_tenant_id_{instance}` — used to build Polaris URNs |
| BulkGetUsers3 | Resolves Polaris user URN by `employeeId` per page — no caching (deleted users handled correctly) |

### DAG fleet

| DAG ID | Role | Concurrency |
|---|---|---|
| `resource_planner_confirmed_bookings_export_{instance}` | **Master**. Snapshots upperBound, fans out pages | `max_active_runs=1` |
| `..._export_child_{instance}_{1,2,3}` | **3 page children**. Each handles pages routed by `pageNumber % 3` | each `max_active_runs=3` |
| `..._op_create_{instance}` | One CREATE per booking (per-page op-DAG fanout) | `max_active_runs=10` |
| `..._op_update_{instance}` | One PARTIAL update per booking (mixed UPDATE/DELETE days inside `scheduleRules`) | `max_active_runs=10` |
| `..._sync_failure_retry_{instance}` | Re-fires failed pages from the failures table | `max_active_runs=1` |

### Master DAG flow

```
view_dag_run_conf
       │
prepare_metadata_request (reads cursor from Variable)
       │
fetch_batch_metadata (POST /batches — stamps upperBound = NOW)
       │
has_pages? ─No→ advance_cursor → end_task
       │ Yes
compute_page_groups (split [1..pageCount] mod 3)
       │
[trigger_child_1, trigger_child_2, trigger_child_3]
       │
combine_child_run_ids
       │
wait_for_all_children
       │
advance_cursor (Variable.set = upperBound; trigger_rule="all_done")
       │
end_task
```

### Page-child flow

```
capture_page_conf
   │
fetch_page (POST /confirmedBookings)
   │
resolve_user_uris (BulkGetUsers3 by unique employee_ids)
   │
classify_and_build (per-day op routing → creates / updates lists)
   │
has_work? ─No→ end_task
   │ Yes
trigger_creates → wait_for_create_runs → trigger_updates → wait_for_update_runs → resolve_failure → end_task
```

### Op-CREATE DAG flow

```
capture_conf
   │
execute_create (GraphQL createTaskResourceUserAllocation; treats "already exists" as success)
   │
put_task_resource_estimate (GraphQL — required by Polaris)
   │
prepare_mark_pushed → mark_pushed (PATCH /markPushed for this booking's rows)
   │
resolve_failure_if_present → end_task
```

### Op-UPDATE DAG flow

```
capture_conf
   │
execute_update (GraphQL updateTaskResourceUserAllocation with scheduleRules)
   │                                                            ↑
   │             ── UPDATE day: setHours = day.hours             │
   │             ── DELETE day: setHours = 0                     │
   │             (mixed within the same mutation)
   │
prepare_mark_pushed → mark_pushed → resolve_failure_if_present → end_task
```

---

## 1. Pre-requisites

1. **All 5 DAGs unpaused** (master, 3 page-children, both op-DAGs). Plus sync-failure-retry if testing that path.
2. Airflow Variables (per instance):
   - `rp_confirmed_bookings_cursor_{instance}` — **set this manually** to a known timestamp before the first run, e.g.:
     ```bash
     airflow variables set rp_confirmed_bookings_cursor_dev "2026-04-01T00:00:00.000+05:30"
     ```
   - `rp_tenant_id_{instance}` — the Polaris tenant slug
3. **Test data prep:** at least one row in `rp_source` with `outbound_pending_op` set and `last_updated_date > cursor`. See TC-CBE-PREP below.
4. **Polaris connection** is healthy (`replicon_conn_id` in instance config).
5. **Gateway connection** is healthy (`rp_api_conn_id`).

---

## TC-CBE-PREP · Test data setup (run before TC-CBE-01)

The DAG reads rows from `rp_source` whose `outbound_pending_op` is set. You need at least one such row to trigger work.

```sql
-- DEV ONLY — inject one ADD row that the cursor will see
INSERT INTO dbo.rp_source (
    source_booking_id, source_system, work_date, hours, hours_type,
    time_code, labor_code, employee_id, users_user_id, start_date,
    last_updated_date, outbound_pending_op
)
VALUES (
    '<QA-TEST-UUID>',                -- pick any UUID
    'Polaris',
    '2026-06-01',                    -- a future work date so it doesn't affect production
    8.0,                             -- 8 hours
    'Client Project',
    '<project_id>~<task_id>',        -- pick a project/task that EXISTS in Polaris
    'QA',                            -- labor_code (optional)
    '<employee_id>',                 -- must exist in Polaris and have a matching user
    '<users_user_id_optional>',      -- gateway resolves it from BulkGetUsers3 anyway
    '2026-06-01',
    SYSDATETIME(),                   -- last_updated_date — must be > cursor
    'ADD'                            -- ADD / UPDATE / DELETE
);
```

Verify the row was inserted:
```sql
SELECT * FROM dbo.rp_source WHERE source_booking_id = '<QA-TEST-UUID>';
```

Make sure your **cursor** Variable is set to a timestamp **before** `last_updated_date`. If unsure, set it to a far past date:
```bash
airflow variables set rp_confirmed_bookings_cursor_dev "2026-01-01T00:00:00.000+05:30"
```

---

## 2. Test Cases

### TC-CBE-01 · Happy path: single ADD day → Polaris CREATE

**How to test**
1. Complete TC-CBE-PREP with a single ADD row.
2. Trigger DAG `resource_planner_confirmed_bookings_export_dev`.

**Expected (master)**
- `fetch_batch_metadata` returns `pageCount >= 1`, `upperBound = NOW(IST)`.
- `has_pages` → Yes → 1 child triggered for the relevant page.
- `wait_for_all_children` waits.
- `advance_cursor` sets `rp_confirmed_bookings_cursor_dev = <upperBound>`.

**Expected (child)**
- `fetch_page` returns 1 booking.
- `resolve_user_uris` (BulkGetUsers3) returns the user URN for the employeeId.
- `classify_and_build` → 1 create item, 0 updates.
- `trigger_creates` fires 1 op-create-DAG run.
- `wait_for_create_runs` succeeds.

**Expected (op-create)**
- `execute_create` succeeds (totalHours=8.0).
- `put_task_resource_estimate` succeeds (returns `taskResourceEstimateId`).
- `mark_pushed` PATCHes the row → `outbound_pending_op` cleared.

**Verify**
```sql
SELECT source_booking_id, outbound_pending_op FROM dbo.rp_source
 WHERE source_booking_id = '<QA-TEST-UUID>';
```
`outbound_pending_op` should now be **NULL**.

**In Polaris UI**: the allocation should appear under the user's allocations for that task.

---

### TC-CBE-02 · Multi-day ADD booking → single CREATE with collapsed scheduleRules

**How to test**
1. Insert 3 ADD rows for the **same** `source_booking_id`, work_dates D, D+1, D+2, all 8 hours.
2. Set cursor before all `last_updated_date`s.
3. Trigger master.

**Expected**
- 1 booking on the page.
- Op-create receives **one** mutation with `scheduleRules: [{dateRange: D..D+2, setHours: 8}]` (collapsed by `collapse_daily_rows_to_schedule_rules`).
- All 3 rows cleared by markPushed.

**Verify** all 3 rows have `outbound_pending_op = NULL`.

---

### TC-CBE-03 · Mixed UPDATE + DELETE on a single booking → one op-update DAG

**How to test**
1. After TC-CBE-01 / 02, simulate a modification: re-insert the test rows with `outbound_pending_op` set:
   - 2 days UPDATE (hours=6)
   - 1 day DELETE (hours=anything, op=DELETE — sethours=0 will be sent)
   - Bump `last_updated_date` to NOW.

```sql
UPDATE dbo.rp_source SET outbound_pending_op = 'UPDATE', hours = 6.0, last_updated_date = SYSDATETIME()
 WHERE source_booking_id = '<UUID>' AND work_date IN ('<D>', '<D+1>');

UPDATE dbo.rp_source SET outbound_pending_op = 'DELETE', last_updated_date = SYSDATETIME()
 WHERE source_booking_id = '<UUID>' AND work_date = '<D+2>';
```

2. Reset cursor to before the bumped `last_updated_date`s.
3. Trigger master.

**Expected**
- `classify_and_build` produces 1 update item (no creates).
- Op-update DAG sends **one** PARTIAL mutation with `scheduleRules: [{D..D+1, setHours: 6}, {D+2, setHours: 0}]`.
- All 3 rows cleared.

**Verify in Polaris UI:** D and D+1 now show 6h, D+2 shows 0h (or no allocation entry).

---

### TC-CBE-04 · Self-healing: CREATE retry hits "already exists"

**Goal:** if Polaris created the allocation but the response was lost (timeout, container kill), a retry should treat duplicate-error as success.

**How to test** (deliberately simulated)
1. Run TC-CBE-01.
2. **Don't** wait for markPushed. While the op-create DAG is mid-flight, kill the container or block the gateway.
3. The op-create may complete partially: Polaris has the allocation but markPushed didn't run.
4. The row still has `outbound_pending_op = ADD`.
5. Trigger the master again.
6. The op-create fires again for the same booking.

**Expected**
- `execute_create` GraphQL response contains an error like `"allocation already exists"`.
- `handle_create_response` matches the error message → returns success.
- `put_task_resource_estimate` runs (idempotent PUT — no harm).
- `mark_pushed` PATCHes → row cleared.

**Verify** Polaris has only ONE allocation (not a duplicate), and the row is now cleared.

---

### TC-CBE-05 · BulkGetUsers3 resolution: deleted user is skipped

**Goal:** if a row's `employee_id` no longer maps to an active Polaris user, the booking is skipped (not pushed).

**How to test**
1. Insert a row with `employee_id = '<known-deleted-employee>'`.
2. Trigger master.

**Expected**
- `resolve_user_uris` call to `BulkGetUsers3` returns 0 rows for that employeeId (Polaris omitted it due to `dataLoadOptionUri: omit-data-if-insufficient-access-permission` or because user was deleted).
- `classify_and_build` sees no user URI for the booking's ADD days → drops the booking from `creates`, increments `skipped_no_user`.
- The booking's `outbound_pending_op` stays set (next run picks it up if the user comes back).

**Verify**
- `skipped_no_user` count > 0 in the `classify_and_build` log.
- The row's `outbound_pending_op` is still `ADD` after the run.
- No allocation created in Polaris.

---

### TC-CBE-06 · Snapshot window: rows inserted mid-run are NOT picked up by this run

**Goal:** confirm the (`cursor`, `upperBound`] window protects against partial-snapshot reads.

**How to test**
1. Set cursor to T0.
2. Trigger master at T1. `fetch_batch_metadata` stamps `upperBound = T1`.
3. While the master is mid-run (between `fetch_batch_metadata` and the children completing), insert a NEW row with `last_updated_date = NOW()` (which is > T1 by now).
4. Wait for master to complete.

**Expected**
- The new row is **NOT** part of this run (its `last_updated_date > upperBound`).
- The new row's `outbound_pending_op` is **still set** after master completes.
- Next master run will pick it up.

**Verify**
- Original test rows: cleared.
- Newly inserted row: still has `outbound_pending_op` set.

---

### TC-CBE-07 · `has_pages = No` path (no work to do)

**How to test**
1. Make sure NO rows have `outbound_pending_op` set in the snapshot window.
2. Trigger master.

**Expected**
- `fetch_batch_metadata` returns `pageCount = 0`.
- `has_pages` → No → goes directly to `advance_cursor` → end.
- Cursor is still advanced (to NOW), so next run uses the new cursor.
- No child DAGs triggered.

---

### TC-CBE-08 · Cursor advances only after all children complete (or all_done)

**Goal:** confirm cursor isn't advanced if children are still running.

**How to test**
1. Inject enough data that the run takes > 5 min (e.g., 100s of bookings).
2. Watch cursor Variable during the run.

**Expected**
- During the run, cursor stays at the old value.
- Only after `wait_for_all_children` completes (success or fail) does `advance_cursor` fire.
- `advance_cursor` uses `trigger_rule="all_done"` — even if some children failed, cursor advances.

---

### TC-CBE-09 · Sync-failure replay

**Goal:** a failed page → recorded in `rp_export_sync_failures` → retried by `sync_failure_retry`.

**How to test**
1. Inject a failure: break the Polaris connection temporarily, trigger master.
2. Some op-DAG runs fail → their `log_sync_failure` task fires → POSTs to `/confirmedBookings/failures`.
3. Verify failure record exists:
   ```sql
   SELECT * FROM dbo.rp_export_sync_failures WHERE status = 'PENDING_RETRY';
   ```
4. Restore Polaris connection.
5. Trigger `resource_planner_confirmed_bookings_sync_failure_retry_dev`.

**Expected**
- Retry DAG fetches the pending failures.
- Re-fires the page-child DAGs (with `failureId` in conf).
- On success, the child's `resolve_failure_if_present` PATCHes the failure record → status = `RESOLVED`.

**Verify**
```sql
SELECT status FROM dbo.rp_export_sync_failures WHERE id = <FAILURE_ID>;
```
Expect `'RESOLVED'`.

---

### TC-CBE-10 · Polaris API rejects the mutation (bad data)

**How to test**
1. Insert a row with an invalid `time_code` (e.g. `'NONEXISTENT_PROJECT~T-123'`).
2. Trigger master.

**Expected**
- `execute_create` GraphQL response has an error.
- `handle_create_response` raises (not the duplicate case).
- Op-create DAG fails → `log_sync_failure` POSTs to failures table.
- `mark_pushed` does NOT run → row stays with `outbound_pending_op = ADD`.

**Verify**
- Failure record in `rp_export_sync_failures`.
- The bad row still has `outbound_pending_op` set.

---

### TC-CBE-11 · Resync with manual cursor reset

**Goal:** confirm setting the cursor backward forces a re-push.

**How to test**
1. After a successful run, manually rewind the cursor:
   ```bash
   airflow variables set rp_confirmed_bookings_cursor_dev "2026-01-01T00:00:00.000+05:30"
   ```
2. **Manually re-flip** a row's `outbound_pending_op`:
   ```sql
   UPDATE dbo.rp_source
      SET outbound_pending_op = 'ADD', last_updated_date = SYSDATETIME()
    WHERE source_booking_id = '<UUID>';
   ```
3. Trigger master.

**Expected**
- That row is picked up again.
- Op-create's `handle_create_response` catches "already exists" → treats as success.
- Cursor advances to the new upperBound.

---

### TC-CBE-12 · markPushed concurrency safety

**Goal:** if a row gets re-flipped to `outbound_pending_op = UPDATE` between read and markPushed, markPushed must NOT clear it.

**How to test**
1. Inject an ADD row, trigger master.
2. Mid-run (between page read and markPushed), manually flip the row:
   ```sql
   UPDATE dbo.rp_source SET outbound_pending_op = 'UPDATE', hours = 9, last_updated_date = SYSDATETIME()
    WHERE source_booking_id = '<UUID>' AND work_date = '<D>';
   ```
3. Let the run complete.

**Expected**
- `mark_pushed` SQL has `AND outbound_pending_op IS NOT NULL` guard — but since we're flipping to a different non-null value, this **does not protect** against this race in the current implementation. **This test is expected to FAIL** the strict optimistic-lock contract — file a bug if you see the new UPDATE value clobbered. (We discussed this in code review; deferred as future work.)

---

### TC-CBE-13 · Tenant ID variable missing → DAG fails loudly

**How to test**
1. Delete `rp_tenant_id_dev` variable.
2. Inject test rows.
3. Trigger master.

**Expected**
- Page-child's `classify_and_build` raises `KeyError: rp_tenant_id_dev` (or similar) when `Variable.get(...)` is called without a default.
- DAG fails. `log_sync_failure` POSTs a failure record.

**Cleanup**: restore the variable.

---

## 3. Cross-DAG / regression

### TC-CBE-XGEN-01 · No orphan `outbound_pending_op` rows after a run

After a successful master run, ALL rows in the snapshot window should have been processed.

```sql
SELECT COUNT(*) FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND outbound_pending_op IS NOT NULL
   AND last_updated_date <= '<upperBound-just-stamped>';
```

Expect **0** (in steady state). Non-zero = something failed; check `rp_export_sync_failures`.

---

### TC-CBE-XGEN-02 · `time_code` referenced by every booking exists in `rp_source_time_codes`

This DAG doesn't write to `rp_source_time_codes`. It assumes the time codes are populated. Check:

```sql
SELECT DISTINCT a.time_code
  FROM dbo.rp_source a
  LEFT JOIN dbo.rp_source_time_codes t
    ON t.source_system = a.source_system
   AND t.time_code = a.time_code
   AND t.type = 'task'
 WHERE a.source_system = 'Polaris'
   AND a.outbound_pending_op IS NOT NULL
   AND t.time_code IS NULL;
```

Expect **0**. If non-zero, those bookings will fail Polaris validation (invalid taskUri). Fix by running `ensure_project_tasks` for those projects.

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY
-- 1. Mark all test rows as pushed (so they don't get reprocessed)
UPDATE dbo.rp_source SET outbound_pending_op = NULL
 WHERE source_booking_id IN ('<TEST_UUIDs>');

-- 2. Optionally delete the test rows
DELETE FROM dbo.rp_source
 WHERE source_booking_id IN ('<TEST_UUIDs>');

-- 3. Clear sync-failure records for tests
DELETE FROM dbo.rp_export_sync_failures
 WHERE master_run_id LIKE '<test-prefix>%';
```

In Polaris UI: delete the test allocations the DAG pushed.

Reset cursor:
```bash
airflow variables set rp_confirmed_bookings_cursor_dev "<known-good-timestamp>"
```

---

## 5. Sign-off criteria

- [ ] TC-CBE-01 (single ADD) — pass
- [ ] TC-CBE-02 (multi-day ADD collapses) — pass
- [ ] TC-CBE-03 (mixed UPDATE/DELETE) — pass
- [ ] **TC-CBE-04 (self-healing CREATE)** — pass (**critical** — recovery from network drops)
- [ ] **TC-CBE-05 (deleted user skip)** — pass (**critical** — prevents failed mutations)
- [ ] TC-CBE-06 (snapshot window stability) — pass
- [ ] TC-CBE-07 (no-work path) — pass
- [ ] TC-CBE-09 (sync-failure replay) — pass (**critical** — the whole replay story)
- [ ] TC-CBE-10 (loud failure on bad data) — pass
- [ ] TC-CBE-XGEN-01 (no orphans after run) — pass
- [ ] TC-CBE-XGEN-02 (every time_code resolvable) — pass
- [ ] Master DAG end-to-end run < 10 min for ~100 bookings
- [ ] Op-DAG fan-out doesn't OOM workers (check Airflow worker memory)

---

## 6. Known limitations / out of scope

- **markPushed race (TC-CBE-12)**: the current SQL only filters on `outbound_pending_op IS NOT NULL` (not on a `lastUpdatedDate` match). A flipped signal between page-read and markPushed can be silently overwritten. Documented for future hardening.
- **Variable read-vs-write timing**: the cursor is read 3 times per master run (master, child, op-DAG). If someone manually changes the variable mid-run, different tasks may see different cursor values. Mitigated by `max_active_runs_master=1` for normal scheduling; not protected against external Variable edits.
- **Polaris partial response on mutation**: if Polaris returns 200 but `data.createTaskResourceUserAllocation.taskResourceUserAllocation` is null, the DAG considers it a success but logs `totalHours=None`. Currently no test for this anomaly.
- **`projectManagerId` not propagated**: confirmed bookings don't carry PM info; this is a Polaris→RP allocation export concern, not relevant here.
- **Concurrent confirmed-bookings + bulk-allocation runs**: harmless but produces noisy logs. Avoid running simultaneously for clarity.
