# QA Testing — Operational Runbook

**Audience:** QA engineers running a full integration-test cycle on a dev environment
**Last updated:** 2026-05-13 · v1.0

This is the **step-by-step playbook**: open this doc, follow it top to bottom, and you'll exercise every Resource Planner integration in the right order. ~60–90 min start to finish if everything passes.

If you want test-case details (inputs, expected rows, verification SQL), each step links to the per-integration guide. If you need mechanics (how to trigger a DAG, set a Variable, query the DB), see [qa_testing_reference.md](qa_testing_reference.md).

---

## How to use this runbook

**Read once, then run with it in front of you.** It's not summary documentation — it's the script.

### Workstation setup (recommended)

Open these in four side-by-side windows. You'll bounce between them constantly:

| Window | What's in it | Why |
|---|---|---|
| **1. This runbook** | `qa_testing_runbook.md` in VS Code (Ctrl+Shift+V to preview) or browser | Your script |
| **2. Airflow UI** | `https://<airflow-host>/home` | Trigger / inspect DAGs |
| **3. SQL client** | Connected to `ResourcePlanning_development` | Verify DB state after every step |
| **4. Polaris UI** | `https://<tenant>.replicon.com/` | Create test data, trigger webhooks |

Bonus: keep [qa_testing_dashboard.html](qa_testing_dashboard.html) open in a fifth tab to tick off tests as you go (state persists in localStorage).

### How to read each step

Every step has the same shape:

1. **DAG / action** — what to click or run
2. **Guide / test case** — link to the deeper test-case doc
3. **Verify** — SQL query or UI check
4. **Pass gate** — explicit success criteria. If this fails, **stop and fix** before continuing.
5. **Watch the log for** *(when relevant)* — grep patterns that indicate success
6. **If it fails** *(when relevant)* — the most-common failure mode + first-line fix

Don't skip the Pass gate. Skipping a failed step poisons everything downstream and makes the bug 10× harder to find.

---

## Phase 0 — Pre-flight (~5 min)

Do this once at the start of every QA session.

### 0.1 Access check

| Have | Where | If missing |
|---|---|---|
| Polaris UI login | `https://<tenant>.replicon.com/` | Ask the integrations team for a dev tenant account |
| Airflow UI login | `https://<airflow-host>/` | Ask DevOps |
| MSSQL `ResourcePlanning_development` access (read/write) | `ashv2614.ads.deltek.com,1435` | Ask DevOps for SQL creds + firewall whitelist |
| A SQL client (SSMS / Azure Data Studio / DBeaver) | local install | Install before continuing |

### 0.2 Branch + environment confirmation

Confirm with the engineer who hands you the build:

- [ ] Which **branch** is deployed to the dev Airflow? (e.g. `INTAI-33_…`)
- [ ] Which **instance** are you testing against? (`dev`, `dev2`, `staging`)
- [ ] Are there `dummy_*` tables in play? (`dev2` typically writes to `dummy_rp_source_*`) — see [reference §6.3](qa_testing_reference.md#63-dummy-table-convention-dev2-instance)
- [ ] Any feature flags or Variables flipped specifically for this build?

Write the answers at the top of your test-tracking sheet — they affect every step below.

### 0.3 Gateway heartbeat

```bash
curl -k https://199.188.134.93:443/resourceplanning-dev-api/health
# expect: {"status":"healthy"} HTTP 200
```

If this fails, **stop** — every downstream DAG will fail. Page the platform team.

### 0.4 Baseline row counts (snapshot)

Take a "before" snapshot — every later step compares against this.

```sql
-- Run this once at the start of the session. Save the results.
SELECT 'rp_source'              AS tbl, COUNT(*) AS n FROM dbo.rp_source             UNION ALL
SELECT 'rp_source_resources',          COUNT(*)      FROM dbo.rp_source_resources   UNION ALL
SELECT 'rp_source_time_codes',         COUNT(*)      FROM dbo.rp_source_time_codes  UNION ALL
SELECT 'rp_source_timeoff_types',      COUNT(*)      FROM dbo.rp_source_timeoff_types;
```

(Use the `dummy_*` table names if you're on `dev2`.)

### 0.5 Set the run-time variables for the integrations you'll test

Every RP DAG has an `enable_batch_task_*` Variable. It **must be `"false"`** for the DAG to actually run (otherwise it short-circuits). Set the ones you'll use today. See [reference §4.3](qa_testing_reference.md#43-variable-cheat-sheet-per-integration) for the full list.

```
Admin → Variables → Edit
  resource_planner_user_export_enable_batch_task_dev               = false
  resource_planner_timeoff_type_export_enable_batch_task_dev       = false
  resource_planner_timeoff_export_enable_batch_task_dev            = false
  resource_planner_project_task_export_enable_batch_task_dev       = false
  resource_planner_project_task_export_bulk_enable_batch_task_dev  = false
  resource_planner_task_resource_allocation_export_enable_batch_task_dev = false
```

**Verify each variable took effect:**

```bash
# CLI confirmation
airflow variables get resource_planner_timeoff_export_enable_batch_task_dev
# should print: false
```

Or in the UI: refresh the Variables list; the Val column should show `false` (no quotes — Airflow stores it as a string).

> ⚠️ Common slip: typing `False` (capital F) or `"false"` (with quotes). The DAG's `if var == "false"` check is **case-sensitive** and won't tolerate quotes. Type exactly `false`.

### 0.6 Test-data scratchpad

Before you start, claim a set of IDs you'll use for the whole cycle. Fill in this table at the top of your tracking sheet — every later step will reference them. This prevents the "wait, which project was I testing?" problem.

| Field | Example value | Yours (fill in) |
|---|---|---|
| Test user (Polaris) | `Jane QA` / `urn:replicon-tenant:abc:user:U-9999` |   |
| Test project | `QA-PROJECT-2026` / `P-QA-001` |   |
| Test tasks (≥2) | `T-QA-001`, `T-QA-002` |   |
| Test time-off type | `Annual Leave` |   |
| Test booking date(s) | `2026-05-13`, `2026-05-14` |   |
| Test allocation IDs | (created during step 2.2) |   |
| Test confirmed booking ID (RP-side) | (created during step 3.1) |   |
| Session start UTC time | `2026-05-13T09:30:00Z` |   |

Best practice: prefix every test ID with `QA-` so production-shape data is distinguishable at-a-glance and cleanup queries are bulletproof.

✅ **Phase 0 done** when: all access works, gateway is healthy, baseline counts saved, variables verified, scratchpad filled.

---

## Phase 1 — Foundation: connectivity + reference data (~10 min)

These DAGs populate the **lookup tables** the rest of the integrations depend on. Run them first or downstream tests will fail with "user not found" / "time-off type not found" errors.

### Step 1.1 — Connectivity test

**DAG:** `integration_gateway_connectivity_dev`
**Why:** Proves Airflow ↔ Gateway ↔ DB round-trip works end to end.

1. Airflow UI → search `integration_gateway_connectivity_dev` → open.
2. Click **▶ Trigger DAG** (top right). Leave config blank.
3. Wait ~30 seconds. All tasks should turn **green**.
4. **Pass gate:** every task green. If any red, drill into the log — usually a connection-string mismatch on the instance config.

**Watch the log for:**
- `gateway responded 200` — round-trip succeeded
- `Connection reset by peer` / `SSLError` — TLS / network issue, not a DAG issue

**If it fails:** 90% of the time it's (a) gateway URL has wrong port/scheme in the instance config, or (b) the cert at the gateway expired. Check `instances/dev.py` → `gateway_url`, then re-run the curl from §0.3.

### Step 1.2 — User export

**DAG:** `resource_planner_user_export_dev`
**Guide:** [qa_testing_guide_users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) — section *User Export*, test case **TC-USR-01**.

1. Trigger the DAG (no config needed).
2. Wait ~2–5 min. Check the grid — all tasks green.
3. **Verify in DB:**
   ```sql
   SELECT TOP 20 *
     FROM dbo.rp_source_resources
    WHERE source_system = 'Polaris'
    ORDER BY last_updated_date DESC;
   ```
   You should see rows. Compare count vs baseline — should have grown (or stayed the same if no new users in Polaris).
4. **Pass gate:** rows exist for the expected Polaris users; no errors in `polaris_to_rp.upload_resources` task log.

**Watch the log for:**
- `fetched N users from polaris` — pull worked
- `inserted N, updated M resources` — gateway accepted the payload
- `skipped row: missing employee_id` — known soft-skip; harmless unless count is large

**If it fails:**
- Empty result set → check the Polaris user report still exists and is named exactly what `instances/dev.py` → `user_report_template_name` expects.
- 401 / 403 in the log → API credentials for Polaris are stale; rotate them and retry.
- All rows insert but `email` is null → likely the `@deltek.com` email validation rejected `@replicon.com` addresses (project rule). Fix the user record in Polaris, not the DAG.

### Step 1.3 — TimeOff Type export

**DAG:** `resource_planner_timeoff_type_export_dev`
**Guide:** [qa_testing_guide_users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) — section *TimeOff Type*, test case **TC-TOT-01**.

1. Trigger. Wait ~1–2 min.
2. **Verify in DB:**
   ```sql
   SELECT * FROM dbo.rp_source_timeoff_types WHERE source_system = 'Polaris';
   ```
3. **Pass gate:** every active time-off type from Polaris appears with a non-null `name` and `time_off_type_id`.

**Watch the log for:**
- `fetched N timeoff types` — SOAP/LWAPI fetch succeeded
- `upserted N timeoff types via gateway` — DB write succeeded

**If it fails:**
- SOAP fault `InvalidSession` → Replicon session expired; check the connection's credentials.
- 0 types fetched → the LWAPI URL in `instances/dev.py` may point at the wrong tenant.

✅ **Phase 1 done** when: lookup tables populated, no DAG failures.

---

## Phase 2 — Polaris → RP (forward sync) (~25 min)

The forward flow: Polaris is source of truth, RP mirrors changes. Run **bulk first** (it backfills history), then **delta** (it picks up incremental changes), then **webhooks** (event-driven near-real-time).

### Step 2.1 — Project Tasks **bulk**

**DAG:** `resource_planner_project_task_export_bulk_dev`
**Guide:** [qa_testing_guide_project_tasks_bulk.md](qa_testing_guide_project_tasks_bulk.md) — **TC-PTB-01**.

> Critical: this populates `rp_source_time_codes`, which **task allocations** and **time-off bookings** both reference. If this is broken, everything else cascades.

1. Trigger. **Run can take 10–20 min** depending on project/task count.
2. While it runs, watch the log — look for `processed N projects, M tasks`.
3. **Verify in DB:**
   ```sql
   SELECT TOP 50 *
     FROM dbo.rp_source_time_codes
    WHERE source_system = 'Polaris'
      AND type IN ('project','task')
    ORDER BY last_updated_date DESC;
   ```
4. **Pass gate:**
   - `type='project'` row count matches active projects in Polaris.
   - `type='task'` row count matches active tasks (within tolerance of skipped ones — log shows skip reasons).
   - `time_code` values are bare IDs (e.g. `T-501`), **not** URNs (`urn:replicon-tenant:…`).

**Watch the log for:**
- `processed N projects, M tasks` — top-level fetch finished
- `parsed N rows from cellCollection` — TaskListService response parsed correctly
- `skipped: <reason>` — log line per skipped row; large counts here mean a data-shape issue in Polaris

**If it fails:**
- `time_code` rows contain `urn:replicon-tenant:…` instead of bare IDs → the parser fell back to raw `id` instead of `slug`/`textValue`. **Critical** — file P1 immediately; downstream allocations will all orphan.
- `cellCollection` AttributeError → TaskListService response shape changed. Re-pull the schema; the parser walks `cellCollection`, not `cells` or `displayText`.
- Run hangs >30 min → likely fetching one page at a time without pagination. Check the operator's `page_size` config.

### Step 2.2 — Task Resource Allocation **bulk**

**DAG:** `resource_planner_task_resource_allocation_export_dev`
**Guide:** [qa_testing_guide_task_resource_allocation_bulk.md](qa_testing_guide_task_resource_allocation_bulk.md) — **TC-TRA-01**.

> Depends on **2.1** — allocations reference time-codes. If 2.1 was skipped or incomplete, expect orphans here.

1. Trigger.
2. **Verify** — allocations landed:
   ```sql
   SELECT TOP 20 *
     FROM dbo.rp_source
    WHERE source_system = 'Polaris'
      AND hours_type = 'Allocation'
    ORDER BY last_updated_date DESC;
   ```
3. **Verify** — no orphans (this is the test that catches the most regressions):
   ```sql
   -- TC-TRA-XGEN-01: every allocation must reference a known time_code
   SELECT a.source_booking_id, a.time_code
     FROM dbo.rp_source a
     LEFT JOIN dbo.rp_source_time_codes t
       ON t.source_system = a.source_system
      AND t.time_code = a.time_code
      AND t.type = 'task'
    WHERE a.source_system = 'Polaris'
      AND a.hours_type = 'Allocation'
      AND t.time_code IS NULL;
   ```
4. **Pass gate:** zero rows from the orphan query. If non-zero, the JIT (`ensure_project_tasks`) should have backfilled — wait 60s and re-run; if still non-zero, file a bug.

**Watch the log for:**
- `has_missing: projectPresent=… missingTaskIds=…` — JIT branch decision
- `trigger_ensure_project_tasks: dispatching` — JIT was invoked
- `inserted N allocations` — gateway accepted the payload

**If it fails:**
- Orphans persist after 60s wait → check the JIT DAG's most recent run (`resource_planner_ensure_project_tasks_dev`); if it errored, fix that first.
- Allocations land with `time_code` = URN → same parser bug as 2.1; investigate together.

### Step 2.3 — TimeOff Bookings (hourly)

**DAG:** `resource_planner_timeoff_export_report_dev`
**Guide:** [qa_testing_guide_timeoff_bookings.md](qa_testing_guide_timeoff_bookings.md) — **TC-TOB-01**, **TC-TOB-14**, **TC-TOB-18**.

Two ways to test, depending on what you're verifying:

**Option A — natural hourly trigger (real-time test):**
1. Wait for the top of the next hour, or trigger manually.
2. Verify rows appear in `dbo.rp_source` with `hours_type IN ('Absence','Holiday')`.

**Option B — test a specific date with the new conf override (TC-TOB-18):**
1. Trigger DAG with config:
   ```json
   { "modified_date": "2026-05-12" }
   ```
2. Confirm log shows: `using override modified_date='2026-05-12' -> 05/12/2026`
3. Verify in DB — rows for bookings modified on 2026-05-12 should land.

**Verify in DB:**
```sql
SELECT TOP 20 *
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND hours_type IN ('Absence','Holiday')
 ORDER BY last_updated_date DESC;
```

**Pass gates:**
- Rows exist for active time-off bookings.
- No duplicates: `SELECT source_booking_id, work_date, COUNT(*) FROM dbo.rp_source GROUP BY source_booking_id, work_date HAVING COUNT(*) > 1;` returns 0.
- Conf override path works (Option B log line + DB rows for the target date).

**Watch the log for:**
- `get_report_params: using override modified_date='YYYY-MM-DD' -> MM/DD/YYYY` *(Option B)* — confirms override was applied
- `get_report_params: using <yesterday|today>` *(Option A)* — the natural-time branch fired
- `processed N timeoff bookings` — fetch + classification finished

**If it fails:**
- Duplicates appear on `(source_booking_id, work_date)` → DB uniqueness constraint missing. **P1**: the insert-only pattern relies on the constraint.
- Conf override accepted but no rows → the `modified_date` you passed is outside the report's actual data range; pick a recent date with known activity.
- `ValueError: Invalid 'modified_date' …` — your conf JSON had the date in `MM/DD/YYYY` or `DD-MM-YYYY` form. Use ISO `YYYY-MM-DD` exactly.

### Step 2.4 — Project Tasks **delta**

**DAG:** `resource_planner_project_task_export_delta_dev`
**Guide:** [qa_testing_guide_project_tasks_delta.md](qa_testing_guide_project_tasks_delta.md) — **TC-PTD-01**, **TC-PTD-08**.

1. In Polaris UI, **modify** an existing project name (or create a new task on an existing project) — gives the delta something to pick up.
2. Trigger DAG manually (or wait for the next hourly run).
3. **Verify** — your change reflected in `dbo.rp_source_time_codes` within ~1 hour.
4. **Pass gate (TC-PTD-08):** the delta row format must match the JIT row format (lowercase `type`, bare IDs). If you see `type='Project'` (uppercase) anywhere, file a bug.

**Watch the log for:**
- `delta processor: fetched N modified projects since <cursor>` — incremental fetch worked
- `cursor advanced to <ISO timestamp>` — cursor updated for next run

**If it fails:**
- Delta picks up nothing despite your Polaris change → cursor on the delta DAG may be ahead of your change. Check `Variables → resource_planner_project_task_export_cursor_dev`. Roll it back to before your edit and re-trigger.
- Same row arrives in `rp_source_time_codes` twice with mismatched `type` casing → JIT/delta format drift. **P1**: this is exactly what TC-PTD-08 catches.

### Step 2.5 — Allocation webhooks (event-driven)

**DAG family:** `resource_planner_task_alloc_webhook_*_dev` (3 DAGs: created, modified, deleted)
**Guide:** [qa_testing_guide_task_resource_allocation_webhooks.md](qa_testing_guide_task_resource_allocation_webhooks.md) — **TC-WHK-01**, **TC-WHK-02**, **TC-WHK-XGEN-02**.

1. **Trigger via Polaris** (preferred — proves the whole chain):
   - Log into Polaris.
   - Open a project → Allocations.
   - **Modify** one allocation (change hours, change user, etc.).
   - Within ~5 seconds, find a new run in `resource_planner_task_alloc_webhook_modified_dev`.

2. **Verify** — the change reflected in `dbo.rp_source` for that allocation.

3. **Verify no orphans** (TC-WHK-XGEN-02) — same SQL as 2.2 step 3.

**Pass gate:** webhook run completed green, DB updated, no orphans.

**Watch the log for:**
- `received webhook event: <event-type>` — Polaris reached Airflow
- `payload.webhook.data.id = <urn>` — payload parsed
- `dispatched JIT for missing tasks` — JIT correctly invoked when needed

**If it fails:**
- No webhook DAG run appears within 30s → check the webhook receiver is healthy + the Variable `rp_*_webhook_token` matches the bearer token Polaris is sending. See [reference §7.2](qa_testing_reference.md#72-direct-post-to-the-webhook-receiver-faster-for-repeatable-tests).
- Webhook fires but task fails on missing user/project → JIT didn't trigger; check `dispatched JIT` line is present.

### Step 2.6 — JIT (Ensure Project Tasks) sanity

**DAG:** `resource_planner_ensure_project_tasks_dev`
**Guide:** [qa_testing_guide_ensure_project_tasks.md](qa_testing_guide_ensure_project_tasks.md) — **TC-EPT-XGEN-01**.

The JIT runs automatically when 2.2 or 2.5 detects missing time-codes. You shouldn't need to trigger it manually — but check it ran during the cycle:

```sql
-- Find JIT-inserted time-codes (look for very recent inserts during 2.2/2.5 window)
SELECT TOP 20 *
  FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND last_updated_date > '<your phase 2 start time>'
 ORDER BY last_updated_date DESC;
```

**Pass gate (TC-EPT-XGEN-01):** when project_task_export_delta runs after JIT inserted rows, it should be a no-op for those rows (proves canonical row format).

✅ **Phase 2 done** when: forward flows all green, zero orphans, no duplicates.

---

## Phase 3 — RP → Polaris (reverse sync) (~15 min)

The reverse flow: RP user makes confirmed-booking changes in the RP UI, RP pushes them back to Polaris.

### Step 3.1 — Confirmed Bookings Export

**DAG family:** `resource_planner_confirmed_bookings_export_dev` (+ page-children + op-DAGs + sync-failure-retry)
**Guide:** [qa_testing_guide_confirmed_bookings_export.md](qa_testing_guide_confirmed_bookings_export.md) — **TC-CBE-01**, **TC-CBE-XGEN-01**, **TC-CBE-XGEN-02**.

1. **Create test data in RP:** in the RP UI (or via direct DB insert), create a confirmed booking with `outbound_pending_op IN ('insert','update','delete')`.
2. Trigger the export DAG.
3. The DAG paginates: master → page child → operation child. Watch the graph.
4. **Verify in Polaris** — the booking should appear (or be modified/deleted) in Polaris's project allocations.
5. **Verify in DB:**
   ```sql
   -- TC-CBE-XGEN-01: every processed row should have outbound_pending_op cleared
   SELECT COUNT(*) FROM dbo.rp_source
    WHERE source_system = 'ResourcePlanning'
      AND outbound_pending_op IS NOT NULL;
   -- expect 0 after a clean run
   ```
6. **Verify time-codes resolve** (TC-CBE-XGEN-02):
   ```sql
   -- Outbound rows should never reference a missing Polaris task
   SELECT o.source_booking_id, o.time_code
     FROM dbo.rp_source o
     LEFT JOIN dbo.rp_source_time_codes t
       ON t.time_code = o.time_code AND t.type = 'task'
    WHERE o.source_system = 'ResourcePlanning'
      AND t.time_code IS NULL;
   -- expect 0
   ```

**Pass gates:** Polaris reflects the change; `outbound_pending_op` cleared; no unresolved time-codes.

**Watch the log for:**
- `classified N bookings -> X creates, Y updates, Z deletes` — classification output
- `op-DAG triggered: <run_id>` (master) and `polaris response: 200` (op-DAG)
- `cleared outbound_pending_op for source_booking_id=<id>` — DB updated

**If it fails:**
- Master DAG green but op-DAGs red → click into the failed op-DAG; usually a Polaris validation error (overlapping allocation, deleted task).
- Polaris response 401 → bookings export uses different auth than report-read. Check the export-specific Polaris API credentials.
- `outbound_pending_op` not cleared even though op-DAG was green → the gateway's "clear" call failed silently. Inspect the gateway log for the same `source_booking_id`.

### Step 3.2 — Sync-failure retry path (negative test)

**DAG:** `resource_planner_confirmed_bookings_export_sync_failure_retry_dev`

1. Force a failure (e.g. point a booking at a deleted Polaris task). Run 3.1.
2. The op-DAG fails. Verify `outbound_failed_at` is set on that row.
3. Trigger the sync-failure-retry DAG.
4. Fix the underlying issue (re-create the task in Polaris). Retry.
5. **Pass gate:** retry succeeds, row's `outbound_failed_at` clears.

✅ **Phase 3 done** when: reverse flow round-trips cleanly + failure-retry works.

---

## Phase 4 — Cross-cutting checks (~10 min)

These tests span integrations. They catch regressions that single-DAG tests miss.

| Check | SQL / action | Expected |
|---|---|---|
| **TC-PTD-08** — JIT vs delta format alignment | Run delta DAG right after JIT inserted a task. Inspect the new row in `rp_source_time_codes`. | Identical format (lowercase type, bare IDs). No phantom duplicates. |
| **TC-TRA-XGEN-01** — no orphan allocations | (same query as step 2.2.3) | 0 rows |
| **TC-WHK-XGEN-02** — no orphans after webhook | (same query as step 2.5.3) | 0 rows |
| **TC-CBE-XGEN-01** — outbound queue drained | (same query as step 3.1.5) | 0 rows |
| **TC-CBE-XGEN-02** — outbound time-codes resolve | (same query as step 3.1.6) | 0 rows |
| **TC-EPT-XGEN-01** — delta is no-op after JIT | Run delta DAG immediately after JIT. Compare row counts before/after. | Same row count (delta has nothing new to insert for JIT rows). |

If **any** of these fail, stop sign-off and file a P1.

---

## Phase 5 — Wrap-up + sign-off (~5 min)

### 5.1 Sign-off checklist

For each guide listed in [qa_testing_index.md §4](qa_testing_index.md#4-sign-off-bundle-per-release), confirm all critical test cases passed. The dashboard ([qa_testing_dashboard.html](qa_testing_dashboard.html)) tracks these — open it in a browser, mark each test pass/fail/skip; results persist in localStorage.

### 5.2 Final row-count delta

Compare against the Phase 0.4 baseline:

```sql
SELECT 'rp_source'              AS tbl, COUNT(*) AS n FROM dbo.rp_source             UNION ALL
SELECT 'rp_source_resources',          COUNT(*)      FROM dbo.rp_source_resources   UNION ALL
SELECT 'rp_source_time_codes',         COUNT(*)      FROM dbo.rp_source_time_codes  UNION ALL
SELECT 'rp_source_timeoff_types',      COUNT(*)      FROM dbo.rp_source_timeoff_types;
```

Row count growth should match what you triggered. Unexpected growth = something else is running (maybe a stale paused DAG was re-enabled).

### 5.3 Cleanup (if you created test data)

Delete only the rows you created — use the unique IDs / source_booking_ids you noted earlier. See [reference §8](qa_testing_reference.md#8-cleanup-pattern-for-any-test).

### 5.4 Report

Output of a clean run:
- ✅ All critical test cases passed
- ✅ Row counts consistent
- ✅ No orphans, no duplicates, no stuck outbound

If anything failed, file bugs using [qa_testing_index.md §6 checklist](qa_testing_index.md#6-bug-reporting-checklist).

---

## Appendix A — Cheat sheet: full DAG trigger order

Copy-paste-friendly list:

```
1. integration_gateway_connectivity_dev
2. resource_planner_user_export_dev
3. resource_planner_timeoff_type_export_dev
4. resource_planner_project_task_export_bulk_dev
5. resource_planner_task_resource_allocation_export_dev
6. resource_planner_timeoff_export_report_dev
   (also test with conf: {"modified_date":"YYYY-MM-DD"})
7. resource_planner_project_task_export_delta_dev
8. (Trigger a webhook from Polaris UI → modify an allocation)
9. resource_planner_confirmed_bookings_export_dev
10. resource_planner_confirmed_bookings_export_sync_failure_retry_dev (negative test)
```

## Appendix B — Quick-troubleshoot (the 5 most common failures)

| Symptom | First check | Likely cause |
|---|---|---|
| DAG task short-circuits to "Skipped" immediately | Variable `..._enable_batch_task_dev` value | Variable is `"true"` — flip to `"false"` |
| `Connection reset by peer` / TLS error | Gateway health (Phase 0.3) | TLS-side issue or gateway down |
| Allocation rows landed but TC-TRA-XGEN-01 has orphans | `rp_source_time_codes` row count | Project Tasks bulk (step 2.1) was skipped or incomplete |
| Time-off booking duplicates | DB unique constraint on `(source_booking_id, work_date)` | Constraint missing — file P1 |
| `outbound_pending_op` rows stuck after Phase 3 | Op-DAG logs (one per row) | Polaris API rejected the push — usually missing task; see TC-CBE-XGEN-02 |

---

## Appendix C — Catchup safety reminder

If you **unpause a DAG that has been paused for a while**, Airflow will queue backlog runs (one per scheduled interval since the configured `start_date`). With `start_date = 2025-01-01` and an hourly schedule, that's ~3,000 runs. **Verify `catchup=False` is set** in the instance config before unpausing on a real env. See [reference §9](qa_testing_reference.md#9-schedules-in-dev-current).

---

## Appendix D — Test data setup cookbook

How to create the **preconditions** each test needs. Use your scratchpad IDs (§0.6).

### D.1 Create a test user in Polaris

1. Polaris UI → **Administration** → **People** → **Add Person**.
2. Required fields:
   - First / Last name: `QA Test`, `<your initials>`
   - Email: must be `<something>@deltek.com` — `@replicon.com` is blocked by the project's validation rule.
   - Employee ID: any unique value, prefix with `QA-`.
   - Status: **Enabled**.
3. **Save**. Note the URN: `urn:replicon-tenant:<tenant>:user:<id>`.

> Used by: step 1.2 (user export), step 2.5 (allocations webhook user-of-allocation).

### D.2 Create a test project + tasks in Polaris

1. Polaris UI → **Projects** → **New Project**.
2. Required:
   - Name: `QA-PROJECT-<date>`
   - Client: any
   - Status: **In Progress** (or whatever the report template filters by).
3. Add ≥2 tasks under the project. Note both task URNs.

> Used by: step 2.1 (bulk), step 2.4 (delta), step 2.2 + 2.5 (allocations target).

### D.3 Create a test time-off booking in Polaris

1. Polaris UI → log in as the test user → **My Time Off** → **Request Time Off**.
2. Required:
   - Time off type: pick one (e.g. `Annual Leave`)
   - Dates: today + 1 day
   - Duration: 8 hours/day
3. **Submit**. If your tenant requires approval, log in as a manager and approve.
4. Note the booking ID from the URL or the My Requests page.

> Used by: step 2.3 (TimeOff Bookings export).

### D.4 Create a test allocation in Polaris

1. Open the project from D.2 → **Allocations** tab → **Add Allocation**.
2. Required:
   - User: the test user from D.1
   - Task: one of the tasks from D.2
   - Start / End: today, today + 5 days
   - Hours/day: 4
3. **Save**. This fires `ProjectPolarisTaskAllocationCreated` webhook → tests step 2.5.

> Used by: step 2.2 (bulk), step 2.5 (webhook).

### D.5 Create a test confirmed booking in RP (for reverse flow)

The RP UI doesn't always expose this; insert directly when needed:

```sql
-- D.5: minimal RP-side confirmed booking ready to push to Polaris
INSERT INTO dbo.rp_source (
    source_system, source_booking_id, work_date, hours,
    hours_type, time_code, resource_id, outbound_pending_op,
    last_updated_date
) VALUES (
    'ResourcePlanning',
    'QA-BOOK-001',
    '2026-05-13',
    8.0,
    'Allocation',
    'T-QA-001',           -- must exist in rp_source_time_codes
    'U-QA-9999',          -- must exist in rp_source_resources
    'insert',             -- triggers the export op-DAG
    SYSUTCDATETIME()
);
```

> Used by: step 3.1 (confirmed bookings export).

### D.6 Force a failure for the retry path (negative test)

```sql
-- D.6: a booking referencing a deleted/missing Polaris task forces Polaris to 400
UPDATE dbo.rp_source
   SET time_code = 'T-DOES-NOT-EXIST-IN-POLARIS',
       outbound_pending_op = 'update',
       outbound_failed_at = NULL
 WHERE source_booking_id = 'QA-BOOK-001';
```

Trigger the export — the op-DAG should fail and stamp `outbound_failed_at`. Then fix the `time_code` and run the sync-failure-retry DAG (step 3.2).

> Used by: step 3.2 (sync-failure-retry).

---

## Appendix E — DAG run conf JSON examples

Copy-paste these into the **Configuration JSON** field when triggering. Plug in your scratchpad IDs.

### TimeOff Bookings — override the modified date (TC-TOB-18)

```json
{ "modified_date": "2026-05-12" }
```

### TimeOff Bookings — invalid date (TC-TOB-18 negative)

```json
{ "modified_date": "12-05-2026" }
```

Expect: DAG fails fast with `ValueError: Invalid 'modified_date' …`. **This is success** — proves the validation works.

### Ensure Project Tasks (JIT) — manual invocation

```json
{
  "sourceSystem": "Polaris",
  "project_id": "P-QA-001",
  "task_ids": ["T-QA-001", "T-QA-002"]
}
```

### Allocation webhook — direct injection (skip Polaris)

```json
{
  "webhook": {
    "data": {
      "id":     "urn:replicon-tenant:<tenant>:psa-task-allocation:QA-ALLOC-001",
      "project":{ "uri": "urn:replicon-tenant:<tenant>:project:P-QA-001" },
      "task":   { "uri": "urn:replicon-tenant:<tenant>:task:T-QA-001" },
      "user":   { "uri": "urn:replicon-tenant:<tenant>:user:U-QA-9999" },
      "actingUser": { "uri": "urn:replicon-tenant:<tenant>:user:U-QA-9999" }
    }
  }
}
```

Use via `curl -k -X POST` against the webhook receiver (see [reference §7.2](qa_testing_reference.md#72-direct-post-to-the-webhook-receiver-faster-for-repeatable-tests)) — **not** through the UI Trigger DAG button (webhooks expect HTTP POST, not a `dag_run.conf`).

### Confirmed Bookings — sync-failure-retry, replay a specific booking

```json
{ "source_booking_id": "QA-BOOK-001" }
```

---

## Appendix F — Per-integration cleanup SQL

Run after the cycle. **Always pin to your `QA-` prefix** so you don't delete real data.

```sql
-- F.1 Time-off bookings + allocations (rp_source)
DELETE FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id LIKE 'QA-%';

-- F.2 Resources (users)
DELETE FROM dbo.rp_source_resources
 WHERE source_system = 'Polaris'
   AND resource_id LIKE 'U-QA-%';

-- F.3 Time codes (projects + tasks)
DELETE FROM dbo.rp_source_time_codes
 WHERE source_system = 'Polaris'
   AND time_code LIKE '%QA-%';

-- F.4 Time-off types
-- (usually keep — these are pulled fresh from Polaris and not test-specific)

-- F.5 RP-side confirmed bookings (reverse flow)
DELETE FROM dbo.rp_source
 WHERE source_system = 'ResourcePlanning'
   AND source_booking_id LIKE 'QA-%';
```

If you're on `dev2`, replace each table name with its `dummy_` counterpart.

> ⚠️ Run each `DELETE` as a `SELECT` first. Visually confirm the rows are yours, then change `SELECT *` to `DELETE` and re-run. Two-pass deletion is the rule.

---

## Appendix G — Cycle tracker template

Copy this Markdown table into your tracking sheet (or use the [dashboard HTML](qa_testing_dashboard.html)). One row per step in the runbook.

```markdown
| Step | DAG | Started | Finished | Result | Pass gate met? | Notes |
|------|-----|---------|----------|--------|----------------|-------|
| 0.3  | (gateway health)              | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 1.1  | integration_gateway_connectivity_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 1.2  | resource_planner_user_export_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 1.3  | resource_planner_timeoff_type_export_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 2.1  | resource_planner_project_task_export_bulk_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 2.2  | resource_planner_task_resource_allocation_export_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 2.3  | resource_planner_timeoff_export_report_dev | hh:mm | hh:mm | ✅ / ❌ | y/n | which option (A/B)? |
| 2.4  | resource_planner_project_task_export_delta_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 2.5  | resource_planner_task_alloc_webhook_modified_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 2.6  | resource_planner_ensure_project_tasks_dev (auto) | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 3.1  | resource_planner_confirmed_bookings_export_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 3.2  | resource_planner_confirmed_bookings_export_sync_failure_retry_dev | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
| 4.x  | Cross-cutting orphans + format check | hh:mm | hh:mm | ✅ / ❌ | y/n | rows returned (should be 0) |
| 5.x  | Cleanup + sign-off | hh:mm | hh:mm | ✅ / ❌ | y/n |   |
```

For every ❌, capture the bug per [qa_testing_index.md §6 checklist](qa_testing_index.md#6-bug-reporting-checklist).

---

**Found a gap in this runbook?** Edit it — engineering keeps it in lockstep with the DAGs in the same PR that changes behavior.
