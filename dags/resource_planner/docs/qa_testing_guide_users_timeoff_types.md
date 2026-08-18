# QA Testing Guide — User Export & TimeOff Type Export

**Scope:** `resource_planner_user_export_*` and `resource_planner_timeoff_type_export_*`
**Direction:** Polaris → Resource Planner (forward, schedule-driven pull)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0

---

## 0. Context

| | User export | TimeOff Type export |
|---|---|---|
| What it does | Pulls users from Polaris, identifies eligible ones, writes them to RP | Pulls time-off type catalog from Polaris, writes new types to RP |
| Polaris source | `User Report` (CSV) + `eligiblePolarisEmployees` lookup + `resources` lookup | LWAPI service (`RepliconServicePageOperator`) |
| Target table | `dbo.rp_source_resources` | `dbo.rp_source_time_codes` (with `type='timeOffType'`) |
| Gateway endpoint (write) | `POST /api/v1/rp/sourceResources` | `POST /api/v1/rp/sourceTimeCodesTimeOffTypes` |
| Gateway endpoint (read) | `GET /api/v1/rp/eligiblePolarisEmployees`, `POST /api/v1/rp/resources` | `GET /api/v1/rp/sourceTimeCodesTimeOffTypes?sourceSystem=Polaris` |
| Schedule | `0 0 * * *` (daily, midnight) | `0 0 * * *` (daily, midnight) |
| Idempotency | Insert-only — fetches what's missing and adds it. Existing rows are skipped (not updated). | Insert-only — same pattern. |
| Concurrency | `max_active_runs=1` per instance | Default |

Both DAGs follow the same shape:
```
view_dag_run_conf → can_run_batch_task (IfOperator)
                       │  Yes → batch_task → end_task
                       └─ No  → fetch Polaris → fetch existing from RP →
                                identify_users_to_add (compare) →
                                has_any_to_add (IfOperator)
                                   │  Yes → prepare payload → insert → end
                                   └─ No  → end
```

---

## 1. Pre-requisites

### 1.1 Airflow connections (must exist, must be unpaused)

| Connection ID | Used by | Notes |
|---|---|---|
| `replicon_Repliconpincstream6dev_replicon` | Both DAGs (for Polaris calls) | Provided by the Replicon SDK setup |
| `resource_planning_api_connection` | Both DAGs (for gateway writes / reads) | Host: `https://199.188.134.93:443/resourceplanning-dev-api` (or env-specific) |

### 1.2 Airflow Variables (per instance)

| Variable | Used by | Required? |
|---|---|---|
| `resource_planner_user_export_enable_batch_task_dev` | User export | Yes — set to `"false"` to actually run the DAG. `"true"` skips to a no-op batch task. |
| `resource_planner_timeoff_type_export_enable_batch_task_dev` | TimeOff Type export | Yes — same semantics. |

### 1.3 Polaris-side prerequisites

For **User export**:
- At least one user in the Polaris "Resource Planner Project - User Template" report (the report referenced by `config.user_report_name`)
- At least one user that is "eligible" (returned by `/eligiblePolarisEmployees` — based on the `vw_d_staff_replica` view)

For **TimeOff Type export**:
- At least one time-off type defined in Polaris (TimeOff > Setup > Types)

### 1.4 Database baseline check

Before testing, capture the current row counts so you can verify deltas:

```sql
-- User export baseline
SELECT COUNT(*) AS baseline_resources
  FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris';

-- TimeOff Type baseline
SELECT COUNT(*) AS baseline_timeoff_types
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND type = 'timeOffType';
```

---

## 2. User Export — Test Cases

### TC-USR-01 · Happy path: add a brand-new user

**Goal:** confirm that a new Polaris user lands in `rp_source_resources`.

**Setup**
1. Pick a Polaris user that is NOT yet in `rp_source_resources` (or create one in Polaris).
2. Ensure they appear in both:
   - The "Resource Planner Project - User Template" report
   - The eligible-employees lookup (`vw_d_staff_replica`)
3. Note their `Employee_ID`.

**Run**
1. Set Airflow Variable `resource_planner_user_export_enable_batch_task_dev = "false"`.
2. Manually trigger DAG `resource_planner_user_export_dev`.

**Expected**
- DAG completes successfully.
- Task chain followed: `get_eligible_employee_ids → ... → has_any_users_to_add → Yes → prepare_insert_users_request → insert_users_to_rp_source_resources → end_task`.
- `identify_users_to_add` log shows ≥ 1 user.
- `insert_users_to_rp_source_resources` returns HTTP 200.

**Verify**
```sql
SELECT *
  FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris'
   AND employee_id = '<YOUR_EMPLOYEE_ID>';
```
Row should exist with:
- `source_system = 'Polaris'`
- `employee_id` matches the Polaris one
- `users_user_id` populated (from the resources lookup)
- `resource_id` populated

---

### TC-USR-02 · Idempotency: re-run does NOT duplicate

**Goal:** confirm a second run skips users that are already there.

**Setup:** complete TC-USR-01 first (so at least one user exists).

**Run**
1. Re-trigger `resource_planner_user_export_dev` immediately.

**Expected**
- DAG completes successfully.
- `identify_users_to_add` log shows **0 users to add** for the already-synced user.
- `has_any_users_to_add` branches to **No** (end_task path).
- `insert_users_to_rp_source_resources` task is **skipped**.

**Verify**
```sql
SELECT employee_id, COUNT(*) AS n
  FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris'
   AND employee_id = '<YOUR_EMPLOYEE_ID>'
 GROUP BY employee_id;
```
`n` must be **1**, not 2.

---

### TC-USR-03 · Skip path: batch task toggle ON

**Goal:** confirm the variable toggle correctly short-circuits the DAG.

**Run**
1. Set Variable `resource_planner_user_export_enable_batch_task_dev = "true"`.
2. Trigger the DAG.

**Expected**
- `can_run_batch_task` branches to **Yes**.
- `batch_task` runs (no-op) and goes straight to `end_task`.
- `get_eligible_employee_ids` and all downstream Polaris/gateway calls are **skipped**.

**Verify**
- No new rows in `rp_source_resources` (compare against baseline).
- DAG view in Airflow shows the Yes branch was taken.

**Cleanup:** flip the variable back to `"false"` for the next tests.

---

### TC-USR-04 · Empty Polaris result

**Goal:** confirm the DAG handles a no-data scenario gracefully.

**Setup**
1. In Polaris, ensure no users match the "Resource Planner Project - User Template" report (or temporarily filter to a non-existent employee).

**Run**
1. Trigger the DAG.

**Expected**
- `user_report_has_data` (or equivalent has-data check) branches to **No** → `end_task`.
- DAG completes with state `success`, no inserts.

**Verify:** baseline counts unchanged.

---

### TC-USR-05 · Polaris user exists but NOT in eligible-employees list

**Goal:** confirm the eligibility filter blocks non-eligible users.

**Setup**
1. Pick a Polaris user who is in the report but **not** in `vw_d_staff_replica` (typical: contractors, terminated users).

**Run**
1. Trigger the DAG.

**Expected**
- `identify_users_to_add` filters them out (compares against eligible employees).
- That user does **not** appear in `rp_source_resources`.

**Verify**
```sql
SELECT 1
  FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris'
   AND employee_id = '<NON_ELIGIBLE_EMPLOYEE_ID>';
```
Should return 0 rows.

---

### TC-USR-06 · Polaris user has no resource mapping

**Goal:** confirm users without a `usersUserId` resource mapping still get inserted (with empty `usersUserId`).

**Setup**
1. Pick a Polaris user whose `employee_id` is in the eligible set but **not** in `/api/v1/rp/resources` (no existing mapping).

**Run**
1. Trigger the DAG.

**Expected**
- User is added with `users_user_id` and `resource_id` as empty strings (not NULL — string defaults).

**Verify**
```sql
SELECT users_user_id, resource_id
  FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris'
   AND employee_id = '<UNMAPPED_EMPLOYEE_ID>';
```
Both columns should be `''` (empty string).

---

### TC-USR-07 · Gateway connection failure

**Goal:** confirm the DAG fails loudly when the gateway is unreachable.

**Setup**
1. Temporarily disable the gateway service (or change the Airflow Connection host to an unreachable IP).

**Run**
1. Trigger the DAG.

**Expected**
- `get_eligible_employee_ids` (or `fetch_resource_map`) **fails** with a `ConnectionError` / `503`.
- DAG state: **failed**.
- Airflow retries kick in (`retries=2`); if all retries fail, DAG ends in `failed`.

**Cleanup:** restore the gateway. Re-trigger to confirm recovery.

---

### TC-USR-08 · Schedule fires (no manual trigger)

**Goal:** confirm the daily cron actually fires.

**Setup**
1. Ensure DAG is unpaused.
2. Set start_date in the past (already done in `instances/dev.py`).
3. **Critical:** verify `catchup=False` in the DAG definition (otherwise it'll backfill from 2025-01-01).

**Run**
1. Wait for `0 0 * * *` (midnight UTC) or set the system clock close to it.

**Expected**
- Exactly **one** DAG run fires at midnight.
- Run is named `scheduled__<date>T00:00:00+00:00`.

---

## 3. TimeOff Type Export — Test Cases

### TC-TOT-01 · Happy path: add a brand-new time-off type

**Goal:** confirm a new Polaris time-off type lands in `rp_source_time_codes`.

**Setup**
1. In Polaris (TimeOff > Setup > Types), create a new type: e.g. `QA-Test-TimeOff`.
2. Confirm it has `enabled=true` and an internal Polaris ID.

**Run**
1. Set `resource_planner_timeoff_type_export_enable_batch_task_dev = "false"`.
2. Manually trigger `resource_planner_timeoff_type_export_dev`.

**Expected**
- DAG completes successfully.
- `identify_timeoff_types_to_add` log shows 1 type to add.
- `has_any_timeoff_types_to_add` branches **Yes** → insert path.
- `insert_timeoff_types` returns HTTP 200.

**Verify**
```sql
SELECT time_code, time_code_name, parent_time_code, type, time_entry_enabled
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND type = 'timeOffType'
   AND time_code_name = 'QA-Test-TimeOff';
```
Row should exist with:
- `time_code` = the Polaris type ID
- `parent_time_code` = same as `time_code`
- `task_level = 0`
- `time_entry_enabled` = 1 (since Polaris had `enabled=true`)

---

### TC-TOT-02 · Idempotency: re-run does NOT duplicate

**Goal:** same as TC-USR-02 but for time-off types.

**Setup:** TC-TOT-01 must be complete.

**Run**
1. Re-trigger the DAG.

**Expected**
- `identify_timeoff_types_to_add` log shows **0 types to add**.
- `has_any_timeoff_types_to_add` branches **No** → end.
- No insert task runs.

**Verify**
```sql
SELECT time_code, COUNT(*) AS n
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND type = 'timeOffType'
   AND time_code_name = 'QA-Test-TimeOff'
 GROUP BY time_code;
```
`n` must be **1**.

---

### TC-TOT-03 · Polaris type with `enabled=false`

**Goal:** confirm disabled types still get inserted but `time_entry_enabled` is 0.

**Setup**
1. In Polaris, set the test type to `enabled=false` (or create a new disabled one).

**Run**
1. Re-trigger the DAG. (Will only fire for *new* types — if you re-enabled then disabled the same type, it won't update — that's by design, insert-only.)

**Expected**
- For a brand-new disabled type, row is created with `time_entry_enabled = 0`.

**Verify**
```sql
SELECT time_entry_enabled
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND type = 'timeOffType'
   AND time_code_name = '<DISABLED_TYPE_NAME>';
```
Expect `0`.

---

### TC-TOT-04 · Skip path: batch task toggle ON

Same as TC-USR-03 but for `resource_planner_timeoff_type_export_enable_batch_task_dev`. No new rows expected.

---

### TC-TOT-05 · LWAPI pagination

**Goal:** confirm the `RepliconServicePageOperator` correctly pages through all types when the catalog is large.

**Setup**
1. Confirm Polaris has > 1 page worth of time-off types (typical pagesize is 100; check `main.py` for actual value).
2. If the dev tenant doesn't have enough types, this case can be skipped in dev and validated in QA env.

**Run**
1. Trigger the DAG.

**Expected**
- `get_all_timeoff_types` task succeeds, makes multiple page requests.
- All types from all pages flow into `identify_timeoff_types_to_add`.

**Verify**
- Compare count in RP vs count in Polaris (manual SQL/Polaris UI check).

---

### TC-TOT-06 · Empty Polaris catalog

**Goal:** confirm no-data scenario.

**Setup**
1. (Hard to do in shared env — typically skipped unless against a fresh tenant.)

**Run**
1. Trigger the DAG.

**Expected**
- `identify_timeoff_types_to_add` returns 0.
- `has_any_timeoff_types_to_add` → No → end.
- No rows inserted.

---

### TC-TOT-07 · Type name collision (same name, different Polaris IDs)

**Goal:** confirm uniqueness is enforced by `time_code` (the Polaris ID), not by `time_code_name`.

**Setup**
1. In Polaris, create two types with the same `name` but they'll get different internal IDs.

**Run**
1. Trigger the DAG.

**Expected**
- Both types are inserted as separate rows in `rp_source_time_codes` (distinct `time_code` values).
- Names collide visually but the PK uniqueness holds.

**Verify**
```sql
SELECT time_code, time_code_name, COUNT(*) OVER (PARTITION BY time_code_name) AS n_with_same_name
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND type = 'timeOffType'
   AND time_code_name = '<COLLIDING_NAME>';
```
Expect 2 rows, both with `n_with_same_name = 2` but distinct `time_code` values.

---

### TC-TOT-08 · Gateway connection failure

Same as TC-USR-07 but for the time-off type DAG.

---

### TC-TOT-09 · Schedule fires (no manual trigger)

Same as TC-USR-08 — verify daily cron fires at midnight UTC.

---

## 4. Cross-DAG / Regression checks

### TC-XGEN-01 · Both DAGs run on the same daily cadence

**Goal:** sanity-check that having both daily DAGs doesn't cause resource contention.

**Run**
- At `0 0 * * *`, both DAGs fire simultaneously.

**Expected**
- Both succeed.
- Gateway handles the parallel load (single FastAPI process should be fine for these small inserts).
- No deadlock errors in gateway logs (`backend/applog.txt` or equivalent).

---

### TC-XGEN-02 · `dummy_*` table override (dev2 instance)

**Goal:** confirm `targetTable` override routes writes to the dummy testing table.

**Setup**
1. Use the `dev2` instance: `resource_planner_user_export_dev2`.
2. Confirm `instances/dev2.py` has `rp_api_target_table = 'dummy_rp_source_resources'`.

**Run**
1. Trigger `resource_planner_user_export_dev2`.

**Expected**
- Insert happens against `dbo.dummy_rp_source_resources`, NOT `dbo.rp_source_resources`.

**Verify**
```sql
SELECT COUNT(*) FROM dbo.dummy_rp_source_resources;
SELECT COUNT(*) FROM dbo.rp_source_resources;
```
Only the dummy table's count should increase.

---

## 5. Cleanup / Reset

After a test cycle, to start fresh:

### User export rollback
```sql
-- DEV ONLY — do NOT run in prod
DELETE FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris'
   AND employee_id IN ('<TEST_EMPLOYEE_ID_1>', '<TEST_EMPLOYEE_ID_2>');
```

### TimeOff Type rollback
```sql
-- DEV ONLY
DELETE FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND type = 'timeOffType'
   AND time_code_name LIKE 'QA-Test-%';
```

### Polaris-side cleanup
- Delete the test users / time-off types you created in Polaris if they shouldn't persist.

---

## 6. Sign-off criteria

A test pass requires **all** of these:

- [ ] TC-USR-01, USR-02, USR-03, USR-04, USR-05, USR-06 — pass
- [ ] TC-TOT-01, TOT-02, TOT-03, TOT-04 — pass
- [ ] TC-XGEN-02 (dev2 override) — pass
- [ ] No `WARN` lines in DAG logs that aren't covered by an expected failure case
- [ ] DAG durations are within reason (User export < 5 min for a typical tenant; TimeOff Type export < 1 min)
- [ ] Gateway logs show no 5xx errors during the test window

Tests that require Polaris-side admin actions (TC-USR-07, TC-TOT-08 — failure injection) can be done by ops in a dedicated session and don't need to repeat per QA cycle.

---

## 7. Known limitations / out of scope

- **Updates / deletes**: both DAGs are **insert-only**. If a Polaris user is renamed or a time-off type is renamed, RP keeps the old row. There's no test case for that because there's no flow that handles it.
- **Eligibility view refresh**: `vw_d_staff_replica` is the eligibility source for users. If it's stale, a user might be missing or extra. That's an upstream concern — not tested here.
- **Webhooks**: neither DAG is webhook-driven. They only fire on schedule or manual trigger.
