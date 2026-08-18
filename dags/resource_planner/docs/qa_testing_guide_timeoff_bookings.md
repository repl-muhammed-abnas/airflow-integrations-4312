# QA Testing Guide — TimeOff Bookings Export

**Scope:** `resource_planner_timeoff_export_report_*`
**Direction:** Polaris → Resource Planner (forward, schedule-driven pull)
**Audience:** QA engineers
**Last updated:** 2026-05-13 · v1.2

---

## 0. Context

| | TimeOff Bookings export |
|---|---|
| What it does | Pulls approved/active time-off bookings from a Polaris report **filtered by `ModifiedOnUtcDateRangeFilter`**, transforms them into per-day rows, and writes them as Absence/Holiday allocations in RP. Also runs a parallel "deleted bookings" report (unfiltered) and removes those rows from RP. |
| Polaris source | Two Replicon reports: "TimeOff Booking Report" (active, **date-filtered**) + "TimeOff Deleted Bookings Report" (deletions, **unfiltered**) |
| Active-report date filter | At runtime: <br>• `dag_run.conf['modified_date']` (ISO `YYYY-MM-DD`) if present — **QA override** for reproducible tests<br>• Otherwise, `00:00–01:59 UTC` → filter = **yesterday**<br>• Otherwise, `02:00–23:59 UTC` → filter = **today**<br>The early-morning yesterday-window is a midnight-boundary fix. The conf override is a QA convenience — pass any date and the DAG fetches bookings modified that day, without waiting for the real time window. |
| RP read | `POST /api/v1/rp/users` (for `usersUserId` lookup by `employeeId`) |
| RP write — insert | `POST /api/v1/rp/sourceAllocations` |
| RP write — delete | `DELETE /api/v1/rp/sourceAllocations` (by `sourceBookingIdPrefix`) |
| Target table | `dbo.rp_source` (with `hours_type = 'Absence'` or `'Holiday'`) |
| Schedule | `0 * * * *` (hourly, top of hour) |
| Idempotency | Insert-only on the active path. The 00:00 and 01:00 runs re-fetch yesterday's full date, so every booking is processed at least twice. Requires DB-side de-dup on `(source_booking_id, work_date)` to avoid duplicates — confirm with TC-TOB-04. |
| Concurrency | `max_active_runs = 1` per instance |

### Flow (simplified)

```
can_run_batch_task ──Yes──► batch_task ───────────────► end_task
                  │
                  └─No──► get_report_details ──► run_timeoff_report ──► is_report_failed
                                                                          │
                          (parallel) get_deleted_report_details ──► run_deleted_report ──► is_deleted_report_failed
                                                                          │
                          load_report_data ──► create_report_collection
                          load_deleted_report_data ──► create_deleted_collection
                                                                          │
                          fetch_user_id_map (POST /rp/users) ─┐
                                                              │
                          identify_records_to_process  ◄──────┘
                          (groups by booking, computes hours_type,
                          derives source_booking_id, converts MST→UTC)
                                                                          │
                          has_records_to_insert ──Yes──► prepare_insert_payload ──► insert_records (POST /sourceAllocations)
                                                  │
                                                  └─No──► has_skipped_records
                                                                          │
                          identify_deleted_bookings ──► has_records_to_delete ──Yes──► prepare_delete_payload ──► delete_records (DELETE /sourceAllocations)
                                                                                                   │
                                                                                                   └─No──► end_task
```

### Key transformations (these drive the test cases)

| Field in `rp_source` | Derived from |
|---|---|
| `source_booking_id` | `timeoff_booking_uri.split(':')[-1]` (the UUID — same value for every day of a multi-day booking) |
| `source_system` | Constant `'Polaris'` |
| `time_code` | `timeoff_type_uri.split(':')[-1]` |
| `users_user_id` | Looked up from `POST /rp/users` by `employee_id` |
| `hours` | `float(timeoff_hrs)` — booking is skipped if 0 |
| `work_date` | `timeoff_date` (one row per day) |
| `hours_type` | `'Holiday'` if `'holiday'` (case-insensitive) appears in `timeoff_type_name`, else `'Absence'` |
| `last_updated_date` | `modified_on` parsed in MST (America/Denver) → converted to UTC ISO-8601 |
| `employee_id` | from report |

---

## 1. Pre-requisites

### 1.1 Airflow connections

| Connection ID | Used for |
|---|---|
| `replicon_*` (per instance) | Running both Replicon reports |
| `resource_planning_api_connection` | Gateway lookups + writes |

### 1.2 Airflow Variables (per instance)

| Variable | Required value to actually run | Notes |
|---|---|---|
| `resource_planner_timeoff_export_enable_batch_task_dev` | `"false"` | `"true"` (or unset) routes to the no-op batch task |

### 1.3 Polaris reports must exist and be named correctly

Confirm these reports exist and the names match `config.report_name` / `config.deleted_report_name`:

| Config field | Default report name |
|---|---|
| `report_name` | `"TimeOff Booking Report"` (or whatever's set in `instances/dev.py`) |
| `deleted_report_name` | `"TimeOff Deleted Bookings Report"` |

Both must include the columns referenced in `create_report_collection` (active) and `create_deleted_collection` (deleted) — see code for the canonical list.

### 1.4 Baseline DB counts

```sql
-- TimeOff Bookings baseline
SELECT
    COUNT(*)                                   AS total_timeoff_rows,
    COUNT(DISTINCT source_booking_id)          AS unique_bookings,
    SUM(CASE WHEN hours_type='Absence' THEN 1 ELSE 0 END) AS absence_rows,
    SUM(CASE WHEN hours_type='Holiday' THEN 1 ELSE 0 END) AS holiday_rows
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND hours_type IN ('Absence', 'Holiday');
```

---

## 2. Test Cases

### TC-TOB-01 · Happy path: new single-day Absence booking

**Goal:** confirm a brand-new approved single-day time-off booking lands as one row.

**Setup**
1. In Polaris, create a time-off booking for an employee:
   - Type: any non-holiday type (e.g. "Sick Leave", "Personal")
   - Date: one specific day
   - Hours: 8.0
   - Get it to a state that puts it into the "TimeOff Booking Report" (typically approved).
2. Note the **employee_id**, **TimeOffBookingUri**, and **TimeOffTypeUri**.

**Run**
1. Set `resource_planner_timeoff_export_enable_batch_task_dev = "false"`.
2. Manually trigger `resource_planner_timeoff_export_report_dev`.

**Expected**
- DAG completes successfully.
- `identify_records_to_process` log shows: `1 record(s) to insert`.
- `has_records_to_insert` branches **Yes** → `prepare_insert_payload` → `insert_records` (HTTP 200).

**Verify**
```sql
SELECT *
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id = '<BOOKING_UUID>';
```
Expect exactly **1 row** with:
- `hours_type = 'Absence'`
- `work_date = <booking date>`
- `hours = 8.0`
- `users_user_id` populated
- `time_code = <type-uri last segment>`
- `last_updated_date` in UTC

---

### TC-TOB-02 · Multi-day booking explodes into N rows, same `source_booking_id`

**Goal:** confirm a single booking spanning multiple days produces one row per day, all sharing the booking UUID.

**Setup**
1. In Polaris, create a booking spanning **3 consecutive days**, 8 hours each.

**Run**
1. Trigger the DAG.

**Expected**
- `identify_records_to_process` log: `3 record(s) to insert`.

**Verify**
```sql
SELECT source_booking_id, work_date, hours
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id = '<BOOKING_UUID>'
 ORDER BY work_date;
```
Expect **3 rows**, all with the same `source_booking_id`, distinct `work_date`s, `hours = 8.0`.

> **Note on the schema:** since we removed the `_00001` suffix, the natural key is `(source_booking_id, work_date)`, not `source_booking_id` alone. Confirm with QA that the database has the matching unique constraint, otherwise duplicates from re-runs (TC-TOB-04) won't be caught.

---

### TC-TOB-03 · Holiday classification

**Goal:** confirm types whose name contains "holiday" (case-insensitive) get `hours_type = 'Holiday'`, others get `'Absence'`.

**Setup**
1. Create a booking against a Polaris type whose **name contains** "Holiday" (e.g. "Public Holiday", "India Restricted Holiday (RH)").
2. Create another booking against a type whose name does **not** contain "holiday".

**Run**
1. Trigger the DAG.

**Verify**
```sql
SELECT source_booking_id, time_code_name = '<TYPE_NAME>', hours_type
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id IN ('<HOLIDAY_UUID>', '<NON_HOLIDAY_UUID>');
```
Expect:
- Holiday type → `hours_type = 'Holiday'`
- Non-holiday type → `hours_type = 'Absence'`

**Edge case** to spot-check separately:
- Type name `"HOLIDAY-Spring"` → should still be `'Holiday'` (case-insensitive match).
- Type name `"Public-Holidays"` (plural) → still `'Holiday'` (substring match).

---

### TC-TOB-04 · Re-run idempotency (duplicate detection — CRITICAL)

**Why this matters more now:** the 00:00 and 01:00 UTC runs re-fetch *yesterday's* full date. Every booking will be processed at least twice in 24h (its own day + the next morning's two yesterday-runs). If the DB doesn't dedupe, you get duplicates daily.

**Goal:** verify the DB has a unique constraint on `(source_booking_id, work_date)` — or filing a bug if it doesn't.

**Setup**
1. Complete TC-TOB-01 so one booking exists in RP.

**Run**
1. Re-trigger the DAG immediately.

**Expected outcomes (one of these — note which you see):**
- **A.** `insert_records` returns HTTP 200; verification SQL shows 1 row → DB has a unique constraint, the row was silently skipped. ✅ Acceptable.
- **B.** `insert_records` returns HTTP 200; verification SQL shows 2 rows for the same `(source_booking_id, work_date)` → **DUPLICATES** were inserted. ❌ Filing a bug — this happens daily in prod.
- **C.** `insert_records` returns HTTP 4xx/5xx (PK violation) → DB rejected the duplicate. ❌ DAG should handle this gracefully (currently it'll fail).

**Verify**
```sql
SELECT source_booking_id, work_date, COUNT(*) AS n
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id = '<BOOKING_UUID>'
 GROUP BY source_booking_id, work_date
HAVING COUNT(*) > 1;
```
Expect **0 rows** (no duplicates).

---

### TC-TOB-14 · Midnight-boundary catch (yesterday-window logic)

**Why this matters:** the DAG runs hourly. Without the yesterday-fallback in the first two UTC hours, modifications made at e.g. 23:30 UTC would slip through (yesterday's 23:00 run finished before, today's runs filter for today's date).

**Goal:** verify the date filter correctly switches to "yesterday" between 00:00–01:59 UTC.

**Setup**
1. In Polaris, modify a booking at **23:30 UTC**. Note the `ModifiedOnUtc` timestamp.
2. Let the **23:00 UTC** run finish (it shouldn't include this modification — it ran 30 min earlier).
3. Wait until **00:30 UTC** (within the yesterday-window).
4. Trigger the DAG manually (or wait for the scheduled 00:00 run).

**Expected**
- DAG runs with `target_date = yesterday`.
- The 23:30 modification IS in the report (it has yesterday's UTC date).
- It gets inserted into `rp_source`.

**Verify**
```sql
SELECT source_booking_id, last_updated_date
  FROM dbo.rp_source
 WHERE source_booking_id = '<UUID>';
```
Row should exist; `last_updated_date` reflects the 23:30 modification.

**Spot-check the filter logic by inspecting the DAG run log** for the `run_timeoff_report` task — the `reportParameters.filterValues` JSON should show yesterday's date in `MM/DD/YYYY` format (e.g. `05/12/2026` if running at 00:30 UTC on 2026-05-13).

> **Tip:** if you can't wait for the midnight window, use **TC-TOB-18** (conf override) to simulate the test at any time of day. The override gives identical filter behaviour without needing the clock to cross midnight.

---

### TC-TOB-15 · 02:00 UTC switches back to today's filter

**Goal:** confirm the filter transitions correctly at the 02:00 UTC boundary.

**Setup**
1. Modify a booking at **00:30 UTC today**.
2. Wait until **02:00 UTC**. The scheduled run fires.

**Expected**
- DAG runs with `target_date = today`.
- The 00:30 modification is included (it has today's UTC date).

**Verify** the row exists in `rp_source` after the 02:00 run.

**Edge case sanity check:** a booking modified at **00:30 UTC** is picked up by either:
- The 01:00 UTC run (filter = yesterday — booking has today's date, NOT included) — **missed here**
- The 02:00 UTC run (filter = today — included) ✅

So there's a 1-hour delay for very-early-morning modifications. That's expected behaviour, not a bug.

---

### TC-TOB-16 · `ModifiedOnUtcDateRangeFilter` lookup must succeed

**Goal:** the DAG looks up the filter URI by display text from the report definition. If the Polaris report is changed and the filter is renamed/removed, the DAG must fail loudly.

**Setup (admin-only, hard to engineer in shared dev)**
- In Polaris, edit the "TimeOff Booking Report" filter configuration and disable / rename the `ModifiedOnUtcDateRangeFilter`.

**Run**
1. Trigger the DAG.

**Expected**
- `find_first_by_attr_and_get_attr` returns None.
- DAG raises `ValueError("Could not find ModifiedOnUtcDateRangeFilter in report details")`.
- DAG state: **failed**. No silent fallback to unfiltered fetch.

**Cleanup**: restore the Polaris report filter.

If you can't reconfigure Polaris, this case can be verified by code-review only (look for the `raise ValueError` and the `if not modified_on_filter_uri:` guard).

---

### TC-TOB-18 · QA conf override — manual date selection

**Goal:** the DAG accepts `dag_run.conf['modified_date']` (ISO `YYYY-MM-DD`) and uses it as the filter target — bypassing the time-of-day logic. This is the primary mechanism for QA to test date-specific scenarios at any time.

**How to test**
1. **Polaris:** identify a booking known to be modified on a specific historical date (e.g. `2026-05-10`). Note its `ModifiedOnUtc`.
2. **Airflow:** trigger `resource_planner_timeoff_export_report_dev` with conf:
   ```json
   {
     "modified_date": "2026-05-10"
   }
   ```
3. Wait for the DAG to complete.

**Expected**
- DAG run log shows: `get_report_params: using override modified_date='2026-05-10' -> 05/10/2026`.
- `run_timeoff_report` task fires the report with `filterValues` using `05/10/2026`.
- Only bookings modified on `2026-05-10` UTC land in `rp_source`.
- The yesterday/today branch is **not** taken (regardless of current UTC hour).

**Verify**
```sql
SELECT source_booking_id, last_updated_date, work_date
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id = '<UUID-MODIFIED-2026-05-10>';
```
Row(s) present. Compare against the same date's modifications in Polaris UI — they should match.

**Invalid date sub-test**
1. Trigger DAG with `{"modified_date": "not-a-date"}` or `{"modified_date": "13/05/2026"}` (wrong format).
2. **Expected:** task `run_timeoff_report` fails with `ValueError: Invalid 'modified_date' in dag_run.conf: ...`. Loud failure, no silent fallback.

**Practical uses for QA**
- Test a specific historical day's bookings without waiting for the real time window.
- Replay a date that hit a known production issue.
- Validate that bookings modified on day D actually land in RP after the day-D run.

---

### TC-TOB-17 · Backfill gap when DAG is paused

**Why this matters:** previously the DAG fetched the full report each run, so a paused/broken DAG would auto-recover when resumed. Now the date filter means a multi-day gap is **NOT** auto-recovered — modifications during the gap are silently missed.

**Goal:** document the gap. Decide on a recovery procedure.

**Setup**
1. Pause the DAG.
2. In Polaris, modify a booking on day D.
3. Resume the DAG on day D+2.
4. Trigger the DAG.

**Expected**
- The DAG runs with `target_date = today (= D+2)` or `yesterday (= D+1)` depending on the hour.
- The day-D modification is **NOT** in the report (filter excludes it).
- It stays missing until manual intervention.

**Recovery options:**
- **For a single missed day:** manually trigger with `{"modified_date": "<missed-date>"}` (see TC-TOB-18). Cheap and reproducible.
- **For multiple missed days:** trigger once per missed day with the override. A small wrapper script or a cron of manual triggers can drain a multi-day gap.
- **For a wide window (>1 week)**: consider building a bulk DAG, or extending the conf to accept a date range. Out of scope for this release.

This test is informational — the day-by-day replay via TC-TOB-18 is the current recommended recovery path.

---

### TC-TOB-05 · Booking deleted in Polaris → row removed from RP

**Goal:** confirm the deleted-bookings report drives a `DELETE /sourceAllocations` that wipes all days of the deleted booking from RP.

**Setup**
1. Complete TC-TOB-02 (3-day booking in RP).
2. In Polaris, delete that booking. Ensure it appears in the "TimeOff Deleted Bookings Report" (with the Time Off ID populated — that's the booking UUID).

**Run**
1. Trigger the DAG.

**Expected**
- `identify_deleted_bookings` log shows `delete_count = 1`.
- `has_records_to_delete` branches **Yes** → `prepare_delete_payload` → `delete_records` (HTTP 200).

**Verify**
```sql
SELECT COUNT(*)
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id = '<DELETED_BOOKING_UUID>';
```
Expect **0 rows** (all 3 days deleted).

**Note on prefix match:** the delete payload uses `sourceBookingIdPrefix`. After removing the `_00001` suffix, the gateway likely matches `LIKE '<UUID>%'`. Confirm with backend that the DELETE actually fires and matches rows where `source_booking_id = <UUID>` exactly (no false positives against unrelated bookings whose IDs happen to begin with the same chars).

---

### TC-TOB-06 · Skipped: missing `employee_id`

**Goal:** confirm rows without an `employee_id` are skipped (not inserted) and surfaced in skipped count.

**Setup**
1. (Hard to engineer in Polaris — typically appears for orphaned report rows or contractors.) Skip this case unless you can produce it; if your Polaris tenant doesn't have anonymous rows, this case is N/A.

**Verify (if you can engineer it)**
- `identify_records_to_process` log shows `skipped_count > 0`.
- The booking does **not** appear in `rp_source`.

---

### TC-TOB-07 · Zero-hour booking is skipped from insert

**Goal:** confirm bookings with `Time Off Hrs = 0` don't create rows.

**Setup**
1. In Polaris, create a booking with 0 hours (admin override or via API). Or wait for one to occur naturally.

**Run**
1. Trigger the DAG.

**Expected**
- Booking does not appear in `rp_source`.
- `identify_records_to_process` log does not count it in the insert list.

**Verify**
```sql
SELECT 1 FROM dbo.rp_source
 WHERE source_booking_id = '<ZERO_HOUR_UUID>';
```
Expect **0 rows**.

---

### TC-TOB-08 · MST → UTC timezone conversion on `Modified On`

**Goal:** confirm `last_updated_date` is correctly converted from MST (America/Denver) to UTC.

**Setup**
1. Pick a booking whose `Modified On` in the report is a known MST time (e.g. `2026-05-12 09:00:00` MST).

**Verify**
```sql
SELECT last_updated_date
  FROM dbo.rp_source
 WHERE source_booking_id = '<KNOWN_BOOKING_UUID>'
   AND work_date = '<KNOWN_DATE>';
```
Expect the value to be **6 or 7 hours ahead** of the MST timestamp, depending on daylight savings.

E.g.: `2026-05-12 09:00 MST` → `2026-05-12 15:00 UTC` (during MDT/summer) or `16:00 UTC` (during MST/winter).

---

### TC-TOB-09 · Type-name collision (same name, different `time_code`)

**Goal:** confirm `time_code` (from `TimeOffTypeUri`) is the dedup key, not the type name.

**Setup**
1. Two distinct time-off types in Polaris whose human-readable names happen to be identical (rare but possible after a rename).

**Verify**
```sql
SELECT DISTINCT time_code, time_code_name FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id IN (...);
```
Expect 2 distinct `time_code` values.

---

### TC-TOB-10 · Report failure short-circuit

**Goal:** confirm a Polaris report failure causes the DAG to fail loudly via `FailOperator`, instead of silently inserting partial/garbage data.

**Setup**
1. Inject a report failure (e.g. change `config.report_name` to a non-existent report temporarily).

**Run**
1. Trigger the DAG.

**Expected**
- `is_report_failed` branches **Yes** → `fail_report_generation` (`FailOperator`).
- DAG state: **failed**.
- No inserts.

**Cleanup:** revert the config name.

---

### TC-TOB-11 · Skip path: batch task toggle ON (or unset)

**Goal:** confirm the variable toggle short-circuits the DAG.

**Run**
1. Set `resource_planner_timeoff_export_enable_batch_task_dev = "true"` (or delete the variable).
2. Trigger the DAG.

**Expected**
- `can_run_batch_task` branches **Yes** → `batch_task` → `end_task`.
- All Polaris/gateway calls skipped.
- No new rows in `rp_source`.

---

### TC-TOB-12 · Schedule fires hourly

**Goal:** confirm the cron actually fires every hour.

**Setup**
1. Ensure DAG is unpaused.
2. Verify `catchup=False` (otherwise it'll backfill from 2025-01-01 → thousands of runs).

**Run**
1. Wait for `0 * * * *` (top of any hour).

**Expected**
- Exactly one DAG run named `scheduled__<datetime>` fires per hour.
- Concurrent runs blocked by `max_active_runs=1`.

---

### TC-TOB-13 · Gateway connection failure

**Goal:** confirm graceful failure when the gateway is unreachable.

**Setup**
1. Disable the gateway service or break the connection's host.

**Run**
1. Trigger the DAG.

**Expected**
- `fetch_user_id_map`, `insert_records`, or `delete_records` fails with a `ConnectionError` after retries.
- DAG state: **failed**.

**Cleanup:** restore the gateway.

---

## 3. Cross-DAG / regression checks

### TC-TOB-XGEN-01 · Coexistence with Confirmed Bookings reverse flow

**Goal:** confirm time-off bookings (Polaris→RP) and confirmed bookings (RP→Polaris) don't interfere on `rp_source`.

**Why this matters:** `confirmed_bookings_export` reads rows where `outbound_pending_op IS NOT NULL`. The TimeOff Bookings DAG **does not** set `outbound_pending_op` — these rows are read-only consumers of the table.

**Verify**
```sql
SELECT
    SUM(CASE WHEN hours_type IN ('Absence', 'Holiday')
              AND outbound_pending_op IS NOT NULL THEN 1 ELSE 0 END) AS timeoff_with_signal
  FROM dbo.rp_source
 WHERE source_system = 'Polaris';
```
Expect **0**. (TimeOff bookings should never get an `outbound_pending_op` signal — those are only set on actual project allocations that need to be pushed to Polaris.)

---

### TC-TOB-XGEN-02 · `dummy_*` table override (dev2 instance)

Same pattern as in the user-export guide. Trigger `resource_planner_timeoff_export_report_dev2` and confirm writes go to `dbo.dummy_rp_source` (not `dbo.rp_source`).

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY — wipe time-off test rows for a specific booking
DELETE FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id IN ('<TEST_BOOKING_UUID_1>', '<TEST_BOOKING_UUID_2>')
   AND hours_type IN ('Absence', 'Holiday');
```

---

## 5. Sign-off criteria

A test pass requires **all** of these:

- [ ] TC-TOB-01 (single-day insert) — pass
- [ ] TC-TOB-02 (multi-day → N rows, same `source_booking_id`) — pass
- [ ] TC-TOB-03 (Holiday vs Absence classification) — pass
- [ ] **TC-TOB-04 (re-run idempotency)** — pass without duplicates (**critical** — the date filter means re-processing happens daily during the 00:00–01:59 yesterday-window)
- [ ] TC-TOB-05 (delete flow) — pass; rows removed
- [ ] TC-TOB-07 (zero-hour skip) — pass
- [ ] TC-TOB-08 (MST→UTC) — pass
- [ ] TC-TOB-10 (report-failure short-circuit) — pass
- [ ] TC-TOB-11 (batch toggle skip) — pass
- [ ] **TC-TOB-14 (midnight-boundary catch)** — pass (**critical** — proves the yesterday-fallback works)
- [ ] TC-TOB-15 (02:00 UTC switches back to today) — pass
- [ ] TC-TOB-16 (filter lookup fails loudly when missing) — pass
- [ ] TC-TOB-17 (multi-day backfill gap) — **documented**, recovery path agreed with product
- [ ] **TC-TOB-18 (QA conf override)** — pass (**critical** for QA productivity — required for every other date-specific test)
- [ ] TC-TOB-XGEN-01 (no `outbound_pending_op` pollution) — pass
- [ ] DAG duration < 5 min for typical tenant
- [ ] No `WARN` lines in DAG logs that aren't covered by an expected failure case

---

## 6. Known limitations / out of scope

- **Updates to an existing booking** (e.g. hours change, date change without delete+recreate): the DAG re-inserts and may create duplicates. The deleted-bookings report only catches outright deletions, not modifications. If a booking changes in Polaris, the current implementation will leave stale rows in RP until the booking is deleted+recreated.
- **Multi-day backfill gap** (introduced by the date-filter change): if the DAG is paused/broken for ≥ 2 days, modifications during that window are silently missed. The next runs only fetch yesterday/today. **Recovery: manually trigger once per missed day with `{"modified_date": "YYYY-MM-DD"}` in conf — see TC-TOB-17 and TC-TOB-18.**
- **00:00 / 01:00 UTC runs re-process yesterday**: every booking modified on day D is re-fetched by the 00:00 and 01:00 runs of day D+1 (because the filter falls back to yesterday). The DB must dedupe on `(source_booking_id, work_date)` or duplicates will accumulate. See **TC-TOB-04** for verification.
- **Partial-week / fractional bookings**: the test plan assumes whole-day, whole-hour bookings. Fractional hours (e.g. 4.5h) should work but aren't explicitly tested.
- **Approval workflow stages**: this DAG pulls whatever the report returns. If your tenant's report includes "pending approval" bookings, those will land in RP too — confirm with product whether that's intentional.
- **Reports with the wrong column set**: the DAG hard-codes column-name → field mappings. If the report's CSV columns are renamed in Polaris, `create_report_collection` will silently produce empty fields (no error). Out of scope here; would be caught by an end-to-end smoke test.
- **Polaris filter renamed/disabled**: covered by TC-TOB-16. DAG fails loudly via the explicit `ValueError` — no silent fall-back.
