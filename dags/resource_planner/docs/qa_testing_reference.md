# QA Testing — Shared Reference

This doc covers **how** to execute the test cases that the per-integration guides describe. Read it once; then each integration guide references back to specific sections here.

**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0

---

## 1. Tools / access you need before starting

| Tool | URL / location | Why |
|---|---|---|
| **Polaris UI** | `https://<tenant>.replicon.com/` (your dev tenant) | Create test bookings, users, projects, time-off types |
| **Airflow UI** | `https://<airflow-host>/` (ask DevOps for the dev URL) | Trigger DAGs, view logs, set Variables, pause/unpause |
| **Gateway** | `https://199.188.134.93:443/resourceplanning-dev-api` | The FastAPI backend that DAGs write to. You normally **don't** call it directly — use the DAG, check the DB. |
| **MSSQL — `ResourcePlanning_development`** | Host: `ashv2614.ads.deltek.com`, port `1435` | Direct DB access to verify what rows landed. Use SSMS, Azure Data Studio, DBeaver, etc. |
| **Replicon Admin / Polaris API tester** | inside Polaris UI under `Administration > API Browser` | If you need to manually invoke a Polaris service to set up test data |

---

## 2. Logging into Airflow and finding a DAG

1. Open the Airflow UI.
2. Top-left search box → type the DAG ID (e.g. `resource_planner_timeoff_type_export_dev`).
3. Click the DAG name. You'll see the grid/graph view.
4. The **Toggle** at top-left (paused / unpaused) — must be **unpaused** for scheduled runs.

---

## 3. Manually triggering a DAG

### 3.1 Via Airflow UI (most common)

1. Open the DAG page.
2. Click the **▶ Trigger DAG** button (top-right).
3. Optionally pass a `Config JSON`:
   - For most schedule-driven DAGs, leave blank.
   - For DAGs that take input (e.g. `ensure_project_tasks`, sync-failure replay), paste a JSON dict — examples shown in each per-integration guide.
4. Click **Trigger**.
5. The run appears in the grid view. Click any green/red square to drill into individual task logs.

### 3.2 Via Airflow CLI (if you have SSH access)

```bash
airflow dags trigger resource_planner_timeoff_type_export_dev
# with conf:
airflow dags trigger resource_planner_ensure_project_tasks_dev \
  --conf '{"project_id": "P-100", "task_ids": ["T-501"], "sourceSystem": "Polaris"}'
```

---

## 4. Setting Airflow Variables

Most RP DAGs have a `..._enable_batch_task_{instance}` variable that **must** be `"false"` to actually run (otherwise the DAG short-circuits to a no-op).

### 4.1 Via Airflow UI

1. Top menu → **Admin** → **Variables**.
2. Find the variable by key (e.g. `resource_planner_timeoff_type_export_enable_batch_task_dev`).
3. Click the edit (pencil) icon. Set Val to `"false"` (string, no quotes inside the field).
4. **Save**.

### 4.2 Via CLI

```bash
airflow variables set resource_planner_timeoff_type_export_enable_batch_task_dev false
airflow variables get resource_planner_timeoff_type_export_enable_batch_task_dev
airflow variables delete resource_planner_timeoff_type_export_enable_batch_task_dev
```

### 4.3 Variable cheat sheet (per integration)

| DAG | Variable name (replace `{instance}`) |
|---|---|
| User export | `resource_planner_user_export_enable_batch_task_{instance}` |
| TimeOff Types | `resource_planner_timeoff_type_export_enable_batch_task_{instance}` |
| TimeOff Bookings | `resource_planner_timeoff_export_enable_batch_task_{instance}` |
| Project Tasks Delta | `resource_planner_project_task_export_enable_batch_task_{instance}` |
| Project Tasks Bulk | `resource_planner_project_task_export_bulk_enable_batch_task_{instance}` |
| Task Resource Allocation (bulk) | `resource_planner_task_resource_allocation_export_enable_batch_task_{instance}` |
| Task Allocation Webhooks (modified) | `resource_planner_task_alloc_webhook_modified_enable_batch_task` |
| Confirmed Bookings — cursor | `rp_confirmed_bookings_cursor_{instance}` (this one carries the actual cursor, not a toggle) |
| Tenant ID | `rp_tenant_id_{instance}` |

---

## 5. Inspecting task logs

1. In the DAG grid, click the task square (success = green, failed = red, skipped = pink, upstream_failed = orange).
2. Pop-up → **Log** tab.
3. The log shows everything `print()`ed, all API responses (when `log_response=True`), all stack traces.

Useful searches inside a log:
- `WARN:` → expected warnings (e.g. failure logging skipped)
- `ERROR` / `Exception` → real problems
- `classified … bookings -> X creates, Y updates` → classification output for confirmed_bookings_export
- `has_missing: projectPresent=… missingTaskIds=…` → JIT branch decisions

---

## 6. Connecting to MSSQL and verifying data

### 6.1 Connection info (dev)

```
Server:   ashv2614.ads.deltek.com,1435
Database: ResourcePlanning_development
Auth:     Windows / SQL Server (ask DevOps for credentials)
Encrypt:  Trust server certificate (TLS may use a self-signed cert)
```

### 6.2 Common verification queries

```sql
-- See the most recently inserted rows of any source
SELECT TOP 50 *
  FROM dbo.rp_source
 WHERE source_system = 'Polaris'
 ORDER BY last_updated_date DESC;

-- Count by table
SELECT 'rp_source'              AS tbl, COUNT(*) AS rows FROM dbo.rp_source             UNION ALL
SELECT 'rp_source_resources',                   COUNT(*)      FROM dbo.rp_source_resources UNION ALL
SELECT 'rp_source_time_codes',                  COUNT(*)      FROM dbo.rp_source_time_codes;

-- Find rows for a specific booking
SELECT * FROM dbo.rp_source WHERE source_booking_id = '<UUID>';

-- Find duplicates on the natural key
SELECT source_booking_id, work_date, COUNT(*) AS n
  FROM dbo.rp_source
 GROUP BY source_booking_id, work_date
HAVING COUNT(*) > 1;
```

### 6.3 Dummy table convention (`dev2` instance)

The `dev2` instances write to **`dummy_*` tables** so you can test without polluting production-like data. Confirm via the instance config file (e.g. `instances/dev2.py`):

```python
rp_api_target_table = 'dummy_rp_source_resources'
```

If set, the gateway writes to `dbo.dummy_rp_source_resources` instead of `dbo.rp_source_resources`. Verify queries should target the dummy table for those tests.

---

## 7. Triggering a Polaris webhook event (for webhook-driven DAGs)

The task-allocation webhooks fire automatically when actions happen in Polaris. To force one:

### 7.1 Real action in Polaris (preferred)

1. Log into Polaris.
2. Open a project, navigate to **Allocations**.
3. **Add** / **Modify** / **Delete** an allocation for a user.
4. Polaris fires the webhook to Airflow within seconds.
5. Find the triggered DAG run in the Airflow UI under the corresponding receiver DAG.

### 7.2 Direct POST to the webhook receiver (faster for repeatable tests)

```bash
curl -k -X POST "https://<airflow-webhook-receiver>/...path-from-DAG..." \
  -H "Content-Type: application/json" \
  -H "X-Replicon-Webhook-Event-Type: ProjectPolarisTaskAllocationCreated" \
  -H "Authorization: Bearer <token-from-Variable-rp_*_webhook_token>" \
  -d '{
    "webhook": {
      "data": {
        "id":     "urn:replicon-tenant:abc:psa-task-allocation:<UUID>",
        "project":{ "uri": "urn:replicon-tenant:abc:project:P-100" },
        "task":   { "uri": "urn:replicon-tenant:abc:task:T-501" },
        "user":   { "uri": "urn:replicon-tenant:abc:user:U-9000" },
        "actingUser": { "uri": "urn:replicon-tenant:abc:user:U-1" }
      }
    }
  }'
```

> **Note:** the exact webhook URL, the bearer token, and the event type strings are per-instance. Get them from the instance config (`instances/dev.py` → `webhook_*` keys) and `airflow variables get <name>`.

---

## 8. Cleanup pattern for any test

```sql
-- 1. Identify your test row(s) by a unique value you created
SELECT * FROM dbo.<table>
 WHERE source_system = 'Polaris'
   AND <unique field> = '<test value>';

-- 2. Delete
DELETE FROM dbo.<table>
 WHERE source_system = 'Polaris'
   AND <unique field> = '<test value>';
```

For **dev**, deleting is safe. **Never** run a wide DELETE against production-shape tables without a WHERE that pins the exact test rows.

---

## 9. Schedules in dev (current)

| DAG | Schedule |
|---|---|
| `project_task_export_delta_*` | `0 * * * *` (hourly) |
| `timeoff_export_report_*` | `0 * * * *` (hourly) |
| `user_export_*` | `0 0 * * *` (daily, midnight UTC) |
| `timeoff_type_export_*` | `0 0 * * *` (daily, midnight UTC) |
| Everything else | manual / triggered |

### Catchup gotcha

`start_date = 2025-01-01` in instance configs. If `catchup=True` (Airflow default), unpausing a daily DAG will create ~125 backlog runs and an hourly one ~3,000. **Verify `catchup=False` is set** before unpausing on a real env. If you see hundreds of queued runs after unpausing, that's why — pause immediately and mark them success.

---

## 10. When something fails — first 3 checks

Order matters; cheap checks first.

1. **Did the right branch run?** Open the DAG graph; look for unexpected red squares or unexpected skipped (pink) tasks.
2. **What does the failed task's log say?** Usually the last 50 lines. Look for `ERROR`, `Exception`, `Connection reset`, `404`/`400`/`500`.
3. **Is the gateway alive?** `curl -k https://<gateway-url>/health` should return `200 {"status":"healthy"}`. If not, ops issue, not a DAG issue.

If all three look fine but data is wrong, query the DB directly and compare against expectations.

---

## 11. Common verification patterns (snippets to paste)

### Did this DAG run insert anything?

```sql
SELECT TOP 20 *
  FROM dbo.<target_table>
 WHERE source_system = 'Polaris'
 ORDER BY last_updated_date DESC;
```

### Verify the JIT didn't leave orphans

```sql
SELECT a.source_booking_id, a.time_code, a.work_date, a.hours
  FROM dbo.rp_source a
  LEFT JOIN dbo.rp_source_time_codes t
    ON t.source_system = a.source_system
   AND t.time_code = a.time_code
   AND t.type = 'task'
 WHERE a.source_system = 'Polaris'
   AND t.time_code IS NULL  -- orphan: allocation references missing project/task
   AND a.hours_type NOT IN ('Absence', 'Holiday');  -- time-off rows don't need time_codes
```

Expect **0 rows** in steady state. A non-zero count right after an allocation push is fine — the `ensure_project_tasks` DAG should catch up within seconds.

### Diff RP vs Polaris (sanity check)

If the integration claims to have pulled N rows from Polaris:
```sql
SELECT COUNT(*) FROM dbo.rp_source WHERE source_system = 'Polaris' AND <filter for the integration>;
```
Compare against the equivalent count in Polaris (run the same report in the Polaris UI and count). Tolerance: small differences are usually skipped rows (missing employee_id, zero hours, etc.) — check the DAG log's `skipped_count`.
