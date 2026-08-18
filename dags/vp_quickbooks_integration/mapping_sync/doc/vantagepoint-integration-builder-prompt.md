# Build a new Vantagepoint integration — follow the `vp_qbo_vendor_sync` pattern

You are building a new airflow integration that syncs data from an external SaaS system (the "source", e.g. QuickBooks Online, UKG Pro, etc.) into Deltek Vantagepoint ("VP", the "target"), using the RAIL (Replicon Airflow Integration Library) framework. The source is typically a Workato recipe being ported to Airflow.

Work end-to-end using the methodology, patterns, conventions, and workarounds documented below.

---

## INPUTS — ask the user upfront and do not proceed until you have them

Before writing any code, confirm or gather:

1. **`<RECIPE_FOLDER>`** *(required)* — absolute path to the folder containing the Workato recipe JSON files for this integration. Example: `C:\Workspaces\vp_qbo_workato`. If the user doesn't provide it, ask: *"Where are the Workato recipe JSON files located?"*

2. **`<REFERENCE_INTEGRATION_FOLDER>`** *(default: `C:\Workspaces\airflow-integrations\dags\vp_qbo_integration\vendor_sync`)* — absolute path to a previously-implemented airflow integration to use as a structural template. If unspecified, use the default (the vendor_sync integration is the canonical reference for this pattern). Always confirm with the user: *"Use `vp_qbo_vendor_sync` as the structural reference, or do you have a different reference integration in mind?"*

3. **`<NEW_INTEGRATION_NAME>`** *(required)* — short identifier for the new integration. Examples: `customer_sync`, `invoice_sync`, `journal_sync`. Used in DAG IDs, file paths, Variable names.

4. **`<SOURCE_SYSTEM_PREFIX>`** *(required)* — e.g. `vp_qbo` for QuickBooks-to-Vantagepoint, `vp_ukgpro` for UKG-Pro-to-Vantagepoint.

5. **`<TARGET_PARENT_DIR>`** *(required)* — where the new integration files should live. Example: `C:\Workspaces\airflow-integrations\dags\vp_qbo_integration\<new_integration_name>\`.

6. **Recipe entry-point file** — within `<RECIPE_FOLDER>`, identify the polling/trigger recipe (usually starts with `poll_*_created` or similar) and the main orchestration recipe (e.g. `*_to_vantagepoint`). Ask the user which files are the canonical entry points if unclear from naming.

If any input is missing, **stop and ask** before proceeding.

---

## PHASE 1 — DISCOVERY (read-only)

Use the **Plan mode** workflow (Explore agents for parallel discovery, then Plan agent for implementation design). Do not edit code yet.

### 1.1 Analyze the Workato recipes completely

For every recipe file in `<RECIPE_FOLDER>` relevant to the new integration, extract:

- **Trigger** (polling cadence? webhook? schedule?)
- **Source entity** (which QBO/UKG/etc. entity, with what filters)
- **Parameters** passed from the trigger recipe to the sub-recipes
- **Step-by-step flow** with line numbers — every `if`, `else`, `try/catch`, `foreach`, variable assignment, HTTP call, lookup search/insert/update, sub-recipe call. Do NOT summarize away conditionals.
- **Field mappings** — exact source → target field names with any transforms, defaults, or conditional values
- **API endpoints** — distinguish `/api/...` (standard) vs `/vision/...` (different base) endpoints. Many VP integrations have a mix.
- **Lookup tables** — every Workato lookup table the recipe uses, its column schema, and which recipes read vs write to it
- **Gates / skip conditions** — every `IF X.present?`, `is_not_true`, etc.
- **Error handling** — every `catch` block, ErrorMessage variable updates, notification recipe calls

### 1.2 Analyze the reference integration

Read every file in `<REFERENCE_INTEGRATION_FOLDER>`:

- `config.py` — shared constants
- `instances/*.py` — per-instance (dev/qa/devops) configs
- `main_dag.py` — top-level scheduled DAG
- `dispatcher_dag.py` — per-customer dispatcher
- `router_dag.py` — per-record routing (create vs update decision)
- `<entity>_create_dag.py` / `<entity>_update_dag.py` — terminal child DAGs
- `utils/python_callable_method.py` — all shared helpers

Note the DAG ID naming convention, task ID conventions, and how connection IDs flow through `dag_run.conf`.

### 1.3 Identify divergences

Compare the recipe's flow against the reference integration's patterns. Note:
- Which recipe steps map 1:1 to existing DAG tasks
- Which recipe steps need new tasks / helpers
- Which recipe steps can be simplified for the airflow context (e.g. GET-then-decide loops that are no-ops in create flow because the parent record was just POSTed)

### 1.4 Surface unknowns to the user

Before planning, ask about:
- Tenant-specific VP config (autonumber settings, mandatory fields, code-table values)
- Workato lookup table contents (need to be ported to Airflow Variables)
- Connection IDs and which one is the "source" vs "target" in `dag_run.conf.connections`
- Whether the user wants strict recipe parity or pragmatic simplifications for create-context dead branches

---

## PHASE 2 — PLAN

Write a plan file to `C:\Users\<user>\.claude\plans\<plan-name>.md` covering:

1. **Context** — why this integration is being built
2. **Decisions** — locked-in choices (DAG structure, sync scope, lookup data location, matching strategy)
3. **Files to create / modify** — full path list
4. **Task chain diagrams** — text/ASCII flow charts for each DAG
5. **Field mappings** — tables for each POST/PUT body (source → target with transforms)
6. **Lookup-table → Variable mappings** — Workato lookup name + Airflow Variable key + JSON schema
7. **Error handling strategy** — which capture function each catch task uses, what the dispatcher does with them
8. **Reference files to read while implementing** — paths to operator source, recipe sections, reference DAG sections
9. **Verification plan** — manual end-to-end test steps

Get explicit user approval via `ExitPlanMode` before writing any code.

---

## PHASE 3 — IMPLEMENTATION ORDER

Build incrementally. Each step must be testable in isolation. Use the **comment-out pattern** liberally: implement one task, comment out all downstream tasks, get the user to test, uncomment next, repeat.

### Standard 5-DAG hierarchy

```
main_dag (scheduled, every 30 min or hourly)
  ↓ trigger per customer
dispatcher_dag (per-customer, schedule_interval=None)
  ↓ trigger per record
router_dag (per-record, decides create vs update)
  ↓ trigger
  ├──▶ <entity>_create_dag
  └──▶ <entity>_update_dag
```

DAG ID naming: `<source_system_prefix>_<new_integration_name>_<dag_type>_{instance}`
e.g. `vp_qbo_vendor_sync_main_dev`, `vp_qbo_vendor_sync_router_dev`.

### Build order

1. **`config.py`** — shared constants (max_active_runs, execution_timeout_days, initial_sync_time, tenant_email, any defaults). Do **NOT** include per-instance values (region, environment) here — those live in instance files only.

2. **`instances/dev.py`** (and qa, devops as needed) — `instance`, `region`, `environment`, `company_key`, `middleware_conn_id`. Each docstring MUST accurately describe its environment (no copy-paste leftovers).

3. **`main_dag.py`** — fetches integrations from middleware, triggers dispatcher per customer. Standard structure:
   - `get_middleware_auth_token` (SimpleHttpOperator POST `/api/v1/oauth/token`)
     - `response_filter=lambda r: r.json().get('access_token')` — **use `.get()`, not `[]`**
   - `fetch_customers_by_integration` (SimpleHttpOperator GET `/api/v1/integrations`)
     - `response_filter=lambda r: r.json().get('integrations', [])` — empty list short-circuits cleanly
     - `data={'integration_type': '<new_integration_name>', 'status': 'enabled'}`
   - `process_customers` (TriggerDagRunForEachItemOperator → dispatcher per customer)

4. **`dispatcher_dag.py`** — per-customer, fetches changed records, triggers router per record. Standard structure:
   - `prepare_sync_timestamps` — capture `last_sync_time` (from Variable, with `KeyError` fallback to `config.initial_sync_time`) and `current_sync_time = datetime.now(timezone.utc)` (NOT `utcnow()`)
   - Source-system query operator — use `last_sync_time` AND `current_sync_time` as a **half-open interval** `[low, high)` to avoid duplicate processing across runs. The query should be a **Jinja string template**, not a callable. Pattern: `"... WHERE LastUpdatedTime >= '{{ result('prepare_sync_timestamps')['last_sync_time'] }}' AND LastUpdatedTime < '{{ result('prepare_sync_timestamps')['current_sync_time'] }}'"`. If the source operator copies `query` into `query_params` at `__init__`, you may need to add `query_params` to the operator's `template_fields` (one-line change, no method addition needed).
   - `extract_<entity>_list` (PythonOperator — unwraps the response dict)
   - `check_if_<entity>_exist` (IfOperator)
   - `process_<entity>` (TriggerDagRunForEachItemOperator → router) — pass `{**item, 'connections': ..., 'customerId': ...}` in conf
   - `wait_for_router_dag_runs` (WaitForDagRunsSensor, `allowed_states=['success', 'failed']`, `failed_states=[]`)
   - `gather_router_dag_errors` (GatherResultsFromDagRunsOperator, `dagrun_task_id='catch_router_dag_error'`)
   - `has_sync_errors` (IfOperator) → `fail_<integration>_sync` (FailOperator) or `update_last_sync_time`
   - `update_last_sync_time` (PythonOperator) — writes `current_sync_time` to Variable
   - `post_dag_run_details` (PostDagRunDetailsToMiddlewareApiOperator, `trigger_rule='all_done'`)

5. **`router_dag.py`** — per-record. Lookup the firm/record map by source-system ID. Branch:
   - Found row with valid target-system ID → trigger `<entity>_update_dag` (pass `vp_<key>_id` in conf)
   - Not found OR row has empty target ID → trigger `<entity>_create_dag`
   - `collect_triggered_dagrun_id` (PythonOperator, `trigger_rule='all_done'`)
   - `gather_<entity>_dag_errors` (GatherResultsFromDagRunsOperator)
   - `catch_router_dag_error` (PythonOperator, `trigger_rule='all_done'`) — **returns dict, does NOT raise**

6. **`<entity>_create_dag.py`** — for newly-encountered records:
   - POST target resource (e.g. firm)
   - Capture the new ID from the response (XCom)
   - Write to the local map Variable (`add_<entity>_to_map`)
   - **GET the just-created resource** (if the POST response doesn't fully populate autonumbered fields — common with VP)
   - Continue with sub-resources (address, contact, accounting, etc.) using the captured ID
   - Final task: `catch_<entity>_dag_error` (PythonOperator, `trigger_rule='one_failed'`) — returns dict

7. **`<entity>_update_dag.py`** — for records already in the map. Must include:
   - **`has_<target>_id` IfOperator** at the start — checks if `vp_<key>_id` in conf is non-empty. If yes → PUT path. If no (lookup row was stale) → POST fallback path (POST resource + write new ID back to map + GET to fetch autonumbered fields)
   - **`resolve_<entity>_id` PythonOperator** — converges PUT and POST-fallback branches; returns the canonical ID for downstream tasks to reference via `{{ result('resolve_<entity>_id') }}`
   - For each sub-resource: **GET-then-decide** pattern (GET existing → match by recipe's match key → branch PUT existing vs POST new)

8. **`utils/python_callable_method.py`** — all shared helpers. Organize into clear sections:
   - Pluggable lookup helpers (Variable-backed; `_read_lookup_variable`)
   - Local map (entity-to-target-ID lookup) — fields **must match the Workato lookup table column names exactly** (including spaces and capitalization, e.g. `"Is Vendor"` not `"IsVendor"` if the source recipe uses spaces)
   - Validators (test functions for IfOperator)
   - Body builders — one per POST/PUT body. Always `_filter_none(body)` before returning.
   - Error capture functions — return dict `{'error': '...'}`, never raise.

---

## PHASE 4 — ITERATIVE DEBUGGING

When the user runs the DAG and surfaces errors, work them one at a time. Common patterns:

### Tenant config issues

- **Autonumber rejection** (`"Firm Number value ignored, autonumber enabled and override is not allowed"`) — omit the field from POST body OR ask user to disable autonumber on that field in VP config
- **NOT NULL constraints on optional fields** (e.g. `"Cannot insert NULL into Company"`) — omit the field entirely (don't send empty string); VP server-side default may kick in
- **"Please provide a Relationship for table X"** — VP needs a category/type code; ask user to either populate the lookup Variable or make the field non-mandatory in VP config

### API endpoint surprises

- **`/api/` vs `/vision/`** — Vision endpoints bypass the `/api/` prefix entirely. Use `VantagepointAPIOperator` for `/api/*` paths, `VantagepointCustomOperator` for `/vision/*` paths.
- **POST vs PUT body shapes** differ even for the "same" entity — recipe may use a full body on POST and a minimal `{ID, Vendor}` body on PUT. Match the recipe.
- **Toggle field encoding** — VP firm record uses `'Y'`/`'N'` for indicators (VendorInd, ClientInd), but firm-address record uses `'true'`/`'false'` literals for PrimaryInd/Payment/Billing. Inconsistent VP API; match the recipe per field.

### Autonumbered fields not in POST response

Some VP tenants have a workflow that populates fields (e.g. `Vendor`, autonumbered codes) *after* the POST commits. The immediate POST response may have empty values for these. Solution: add a `get_<entity>_after_create` task (GET `/firm/{ClientID}`) right after the POST + capture, and read the autonumbered fields from that response in downstream body builders.

### Operator parameter resolution

If using `rail.<X>Operator` with a callable parameter (e.g. `query=build_query_function`), and the operator stores the param into a non-templated attribute at `__init__` time, the callable won't resolve. Two options:
- **Preferred**: switch to a Jinja string template and add the affected attribute to `template_fields` (1-line operator change)
- **Alternative**: thin subclass in the integration that re-resolves the param in `execute()`

### Error message debugging

When upstream fails (e.g. VP 500 error), the error message reaching `catch_<entity>_dag_error` is whatever `get_error_message()` Jinja macro returns. Make sure your `capture_*_error` functions include enough context for debugging — at minimum, source-system ID AND the display name/label. Format:
```
"<Entity> <id> (<display_name>) - <op> failed: <error_message>"
```
Always handle the "name missing" case gracefully (strip-then-check before formatting).

---

## CONVENTIONS & STANDARDS

### File / DAG naming

| Entity | Pattern | Example |
|---|---|---|
| DAG ID | `<source_prefix>_<integration>_<type>_{instance}` | `vp_qbo_vendor_sync_router_dev` |
| Task ID | `snake_case_verb_noun` | `get_recently_changed_vendors` |
| Variable (per-tenant state) | `<source_prefix>_<integration>_<purpose>_{instance}_{customerId}` | `vp_qbo_vendor_sync_last_run_dev_123` |
| Variable (shared lookup) | `<source_prefix>_<integration>_<purpose>` (flat, no instance) | `vp_qbo_vendor_sync_pay_terms_map` |
| Variable (per-instance config) | `<source_prefix>_<integration>_<purpose>_{instance}` | `vp_qbo_vendor_sync_default_vendor_type_dev` |
| Tags | `['vantagepoint_<source>', '<integration>', '<dag_type>']` | `['vantagepoint_quickbooks', 'vendor_sync', 'router']` |
| Connection ID | `<system>_conn_{instance}` | `middleware_conn_dev` |

### Code rules (from `airflow-integrations/CLAUDE.md`)

- **NEVER create custom operators** — use RAIL operators. Thin subclasses to work around upstream bugs are acceptable but flag them.
- **No `replicon.com` email addresses** in code. Personal emails must be `@deltek.com`.
- **Don't create CLAUDE.md files**.
- For any *report* / *Report* pattern: report template name must be in `config.py`; if not specified, ask the user.

### Python style

- `datetime.now(timezone.utc)` — never `datetime.utcnow()` (deprecated)
- `response.json().get('key', default)` — never `response.json()['key']`
- `_filter_none(body)` before returning request bodies — keeps empty strings; drops `None`s. **But**: if VP rejects empty strings for a NOT-NULL field, set to `None` (so `_filter_none` drops it).
- `op_args` for PythonOperator: list of Jinja-templated strings → rendered at task run time
- Body builders read context via `rail.get_current_context()['dag_run'].conf` and prior XComs via `rail.result('<task_id>')`
- For dict-with-fallback Jinja: `"{{ dag_run.conf.get('A') or dag_run.conf.get('B') or '' }}"` — chain `.get()` with `or` fallbacks

---

## RAIL OPERATOR REFERENCE

Use these as your toolkit. Don't build new operators.

### Vantagepoint operators

| Operator | Path | Use case |
|---|---|---|
| `rail.VantagepointFirmOperator(vp_conn_id, request_method, client_id, filters, request_body, pagination)` | `/api/firm` (POST), `/api/firm/{cid}` (GET/PUT/DELETE) | Firm CRUD |
| `rail.VantagepointFirmAddressOperator(vp_conn_id, request_method, client_id, request_body)` | `/api/firm/{cid}/address` | Address GET (list) / POST (insert). For UPDATE, use FirmOperator PUT with `CLAddress` sub-array |
| `rail.VantagepointFirmBatchOperator` | batch /firm operations | Bulk create/update firms |
| `rail.VantagepointFirmAddressBatchOperator` | batch /firm/{cid} with CLAddress | Bulk address operations |
| `rail.VantagepointEmployeeOperator(vp_conn_id, request_method, employee, filters, request_body)` | `/api/employee` | Employee CRUD |
| `rail.VantagepointContactOperator(vp_conn_id, request_method, contact_id, filters, request_body)` | `/api/contact` | Contact CRUD |
| `rail.VantagepointSettingsListOperator(vp_conn_id, endpoint, request_method, request_body)` | `/api/codeTable/*` | Code tables (job titles, suffixes, vendor categories, pay terms, etc.) |
| `rail.VantagepointAPIOperator(vp_conn_id, endpoint, request_method, filters, request_body, pagination)` | `/api/*` (generic) | Any `/api/*` endpoint not covered above |
| `rail.VantagepointCustomOperator(vp_conn_id, endpoint, request_method, request_body)` | direct host (no `/api/` prefix) | `/vision/*` endpoints and others that bypass `/api` |

**Connection ID parameter**: always `vp_conn_id` (passed via `{{ dag_run.conf.connections.intuit }}` in our setup).

### Control flow / orchestration operators

- `rail.create_airflow_dag(dag_id, schedule_interval, max_active_runs, tags, multi_tenant, default_args, company_key, integration_type)`
- `rail.for_each_instance(create_dag_fn)` — at module level, creates one DAG per instance file
- `rail.PythonOperator(task_id, python_callable, op_args, trigger_rule)`
- `rail.IfOperator(task_id, test, yes_task, no_task, trigger_rule)`
- `rail.Label('text')` — branch labels in `>>` chains
- `rail.TriggerDagRunOperator(task_id, trigger_dag_id, conf, wait_for_completion, execution_timeout, retries)`
- `rail.TriggerDagRunForEachItemOperator(task_id, items, trigger_dag_id, conf, execution_timeout)` — `items` is a callable returning a list; `conf` is a callable taking each item
- `rail.WaitForDagRunsSensor(task_id, dag_runs, allowed_states=['success','failed'], failed_states=[])` — **`failed_states=[]` is intentional**: prevents the sensor from failing regardless of child state; errors are gathered separately
- `rail.GatherResultsFromDagRunsOperator(task_id, dag_runs, dagrun_task_id, flatten=True)`
- `rail.FailOperator(task_id, message)`
- `rail.SimpleHttpOperator(task_id, method, http_conn_id, endpoint, headers, data, response_filter)`
- `rail.PostDagRunDetailsToMiddlewareApiOperator(task_id, middleware_api_base_url, trigger_rule='all_done')`

### Source-system operators (vary by integration)

- **QuickBooks**: `rail.QuickBooksVendorOperator`, `rail.QuickBooksCustomerOperator`, etc. — `intuit_conn_id`, `operation`, `query`/`request_body`
- **UKG Pro**: `rail.UKGProEmploymentOperator`, `rail.UKGProDemographicOperator`, `rail.UKGProGenericOperator` — `ukgpro_conn_id`
- For others, search the `replicon-airflow-library/rail/rail/operators/` tree.

### Trigger rules cheat-sheet

| When you need it... | Use `trigger_rule=` |
|---|---|
| Normal "run if upstream succeeded" | (default, `all_success`) |
| Run after IfOperator branch (one parent skipped, one succeeded) | `'none_failed'` |
| Run only if upstream failed (error capture) | `'one_failed'` |
| Run no matter what (cleanup, post-back) | `'all_done'` |

---

## LOOKUP TABLE PORTING

Workato lookup tables become Airflow Variables. Always **port the field names exactly** as the Workato columns name them (including spaces, capitalization).

Pattern:

```python
def _read_lookup_variable(variable_key, default=None):
    raw = Variable.get(variable_key, default_var=None)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw
```

Then specific lookups call this with their key. Examples:
- Map table (entity ID → target ID): `{"<source_id>": {"FirmID": "...", "QBOID": "...", "Is Vendor": "Y", "Name": "..."}}`
- Lookup table (source code → target code): `{"<source_code>": "<target_code>"}`
- Single value (config property): plain string

The map table (writeable) needs `_write_*_map(map_data)` companion. Both helpers replaceable when Airflow ships a real lookup-table primitive.

---

## ERROR PATTERNS

Three-level error propagation (matches recipe's `try/catch` + error variable + notification recipe pattern):

1. **Level 1** — terminal create/update DAGs: `catch_<entity>_dag_error` (PythonOperator, `trigger_rule='one_failed'`) returns `{'error': '<Entity> <id> (<name>) - <op> failed: <msg>'}`. DAG stays SUCCESS.

2. **Level 2** — router DAG: `catch_router_dag_error` (PythonOperator, `trigger_rule='all_done'`) gathers child errors via `GatherResultsFromDagRunsOperator` and falls back to local `get_error_message()`. Returns dict or None. DAG stays SUCCESS.

3. **Level 3** — dispatcher DAG: `gather_router_dag_errors` reads level 2 results, `has_sync_errors` IfOperator branches to `fail_<integration>_sync` (FailOperator) if any. Dispatcher FAILS so middleware logging captures the failure.

**Critical rule**: capture functions RETURN dict; they do NOT raise. The dispatcher's `WaitForDagRunsSensor` requires children to be in SUCCESS state for the sensor to proceed; error info is collected separately.

---

## VERIFICATION CHECKLIST

Before declaring the integration done:

- [ ] Each instance file's docstring matches its actual environment (no copy-paste mistakes)
- [ ] No `datetime.utcnow()` anywhere
- [ ] All `response.json()` access uses `.get(...)` with sensible defaults
- [ ] All HTTP request bodies use `_filter_none(body)` before sending
- [ ] Lookup Variable field names match Workato column names exactly
- [ ] Map Variable is per-customer-scoped if multi-tenant; flat otherwise (per user's decision)
- [ ] Error capture functions include source-system ID AND name (with graceful missing-name fallback)
- [ ] Connection IDs flow through `dag_run.conf.connections` — no hardcoded conn IDs
- [ ] Dispatcher uses half-open time window `[low, high)` in source-system query
- [ ] `WaitForDagRunsSensor` uses `failed_states=[]` (intentional)
- [ ] All terminal child DAGs return SUCCESS (errors captured via dict, never raised)
- [ ] No CLAUDE.md files created; no replicon.com emails in code

---

## CRITICAL FILES TO READ DURING IMPLEMENTATION

| File | Why |
|---|---|
| `<RECIPE_FOLDER>/*.recipe.json` | Source of truth for business logic, field mappings, conditional gates |
| `<RECIPE_FOLDER>/*.lookup_table.json` | Column schema for lookup tables → Airflow Variable JSON schemas |
| `<REFERENCE_INTEGRATION_FOLDER>/router_dag.py` | Template for routing logic, IfOperator chains, error gather |
| `<REFERENCE_INTEGRATION_FOLDER>/<entity>_create_dag.py` | Body builder pattern, None filtering, error capture |
| `<REFERENCE_INTEGRATION_FOLDER>/<entity>_update_dag.py` | GET-then-decide pattern, fallback POST in update branch |
| `<REFERENCE_INTEGRATION_FOLDER>/utils/python_callable_method.py` | Helper naming and signature conventions |
| `replicon-airflow-library/rail/rail/__init__.py` | List of every RAIL operator exported |
| `replicon-airflow-library/rail/rail/operators/vantagepoint/*` | Constructor signatures, default endpoints, response shapes |
| `airflow-integrations/CLAUDE.md` | Project-level rules |

---

## STARTING SIGNAL

Once inputs are confirmed, your first message to the user should be:

> *I'll build the `<new_integration_name>` integration following the `vp_qbo_vendor_sync` pattern. First, I'll analyze the Workato recipes in `<RECIPE_FOLDER>` and the reference integration at `<REFERENCE_INTEGRATION_FOLDER>` in parallel — read-only — then write a plan for your approval before any code changes.*

Then launch parallel Explore agents per the discovery phase. Do not start coding without an approved plan.
