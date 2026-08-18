# `mapping_sync/` cleanup + performance backlog

Reference doc for the dead-code + perf-optimization audit of
`vp_quickbooks_integration/mapping_sync/` performed on **2026-05-26**.
Each item is independently actionable; the **Status** column tracks
iteration progress as items are landed.

> **Verify before acting** — line numbers reflect the working tree at
> audit time. Re-run the cited `grep` / `Read` to confirm a finding is
> still valid before deleting or refactoring; the file may have shifted
> since.

---

## Backlog index

### Dead code

| # | Item | Severity | Status |
|---|---|---|---|
| D1 | Trim `config.py` — remove `DAGConfig`, `APIConfig`, `CollectionsConfig`, and dead `IntegrationConfig` constants/methods | HIGH | **done** (444→141 lines) |
| D2 | Delete `lookup_default_employee_company` + `lookup_default_home_company` | MEDIUM | **done** (CFG_MIGRATION.md row moved to Deferred with re-author instructions) |
| D3 | Add qa/dev/devops instance files (mirror vendor_sync) + clean shadowed `rail_config.py` defaults | LOW | **done** (3 new instance files; 4 dead shadowed constants removed from rail_config.py) |
| D4 | Delete `transaction_tracking/` placeholder subpackage + DAG | LOW (intentional) | **done** (5 files + dir removed; dispatcher chain shrunk to firm→employee→account→tax→bank_code→validate) |
| D5 | Drop `_table_exists` defensive helper in validator path | LOW | **done** |

### Performance opportunities

| # | Item | Effort | Payoff | Status |
|---|---|---|---|---|
| P1 | Parallelize account + tax + bank_code after employee | Medium | High (~30% post-employee wall-time) | pending |
| P2 | Bulk-fetch existing VP firms / employees / accounts by QBOID | Medium | High (N×3 → 3 VP calls) | **done** (each sync now does 1 paginated GET indexed in memory) |
| P3 | Collapse 4 validators into 1 single-download task | Low | Medium (-3 S3 GETs / run) | pending |
| P4 | Share QBO Accounts fetch between `map_account_code` + `map_bank_code` | Low | Low | pending |
| P5 | Batch `mark_step_status` updates in dispatcher | Low | Low | pending |
| P6 | Verify read-only path used by skip gates | Low | Low | pending |

---

# Dead code — detailed findings

## D1. `config.py` — bulk of the file is unreferenced

**File**: `mapping_sync/config.py` (444 lines).

**Evidence**: Repo-wide grep for every symbol declared in `config.py`
returns matches **only** from `config.py` itself for the symbols below.
Verified via:

```text
Grep `IntegrationConfig\.(INTEGRATION_ID|INTEGRATION_NAME|VERSION|DEFAULT_REGION|SUPPORTED_REGIONS|CORE_MAPPING_TABLES|...etc)` over the entire workspace → 1 file: mapping_sync/config.py
```

### `IntegrationConfig` — what's LIVE (keep)

| Symbol | Line(s) | Consumer |
|---|---|---|
| `S3_INTEGRATION_NAME` | 19 | dispatcher, child DAGs, sync helpers |
| `S3_DEFAULT_CUSTOMER` | 21 | fallback in `get_s3_customer` |
| `S3_CUSTOMER_TEMPLATE` | 27–32 | child DAGs (Jinja-templated operator args) |
| `S3_INTEGRATION_TYPE_TEMPLATE` | 40–42 | child DAGs |
| `VANTAGEPOINT_CONN_ID` | 45 | fallback in `get_conn_ids` |
| `QUICKBOOKS_CONN_ID` | 46 | fallback in `get_conn_ids` |
| `DAG_ID_PREFIX` | 295 | `dag_id()` |
| `dag_id()` | 298 | every DAG file |
| `get_s3_customer()` | 209 | sync helpers |
| `get_s3_integration_type()` | 237 | sync helpers |
| `get_cfg()` | 254 | sync helpers, `apply_premapping_state` |
| `get_conn_ids()` | 278 | sync helpers |

### `IntegrationConfig` — DEAD (delete)

| Symbol | Line(s) | Notes |
|---|---|---|
| `INTEGRATION_ID = '014-503 PSA'` | 13 | |
| `INTEGRATION_NAME = '...'` | 14 | |
| `VERSION = '1.0.0'` | 15 | |
| `DEFAULT_REGION = 'US'` | 49 | region resolved via `CFG_Region` in `build_child_dag_conf` |
| `SUPPORTED_REGIONS` | 50 | |
| `CORE_MAPPING_TABLES` | 53–58 | duplicates `utils/tables.MAP_*_TABLE_NAME`; the latter is the source of truth |
| `TRANSACTION_TRACKING_TABLES` | 61–65 | duplicates `utils/tables.OUTSTANDING_*_TABLE_NAME` |
| `CONFIGURATION_TABLES` | 68–73 | |
| `STATE_MANAGEMENT_TABLES` | 76–78 | |
| `ALL_TABLES` | 81–86 | only referenced inside `config.py` |
| `TABLE_SCHEMAS` | 89–123 | superseded by `utils/tables.*_COLUMNS` |
| `REGIONAL_CONFIGS` | 126–154 | |
| `BATCH_SIZE`, `MAX_RETRIES`, `RETRY_DELAY_SECONDS`, `CONNECTION_TIMEOUT`, `API_RATE_LIMIT_PER_MINUTE` | 157–161 | misleading — suggests retries are configurable, but no code reads these |
| `VALIDATION_RULES` | 164–177 | superseded by `validate_mappings_dag` / `summarize_mapping_validations` |
| `LOG_LEVEL`, `LOG_FORMAT` | 180–181 | |
| `get_regional_config()` | 184 | only callable from removed `REGIONAL_CONFIGS` consumers |
| `get_table_schema()` | 188 | |
| `get_required_fields()` | 194 | |
| `get_unique_constraints()` | 199 | |
| `is_core_table()` | 204 | |
| `get_table_priority()` | 303–314 | |

### Entire classes — DEAD (delete)

| Class | Line(s) |
|---|---|
| `DAGConfig` | 317–363 — `DEFAULT_DAG_ARGS`, `CONNECTION_TRIGGER_CONFIG`, `MAPPING_POPULATION_CONFIG`, `TASK_GROUPS` |
| `APIConfig` | 366–401 — `QUICKBOOKS_API`, `VANTAGEPOINT_API`, `QB_ENDPOINTS`, `VP_ENDPOINTS`. Also note `QUICKBOOKS_API.base_url` hardcodes the **sandbox** URL — would be wrong even if used |
| `CollectionsConfig` | 404–444 — `COLLECTION_SETTINGS`, `INDEX_STRATEGIES`, `OPTIMIZATION_SETTINGS` |

### Suggested fix

Reduce `config.py` to a single ~80-line `IntegrationConfig` class
containing only the LIVE column above. Net delete: ~360 lines.

Also drop the `from typing import Dict, List, Any` import (line 6) if
the remaining code doesn't need it after the trim.

---

## D2. Unused employee-default helpers

**File**: `mapping_sync/utils/python_callable_method.py`

| Function | Line | Status |
|---|---|---|
| `lookup_default_employee_company(instance)` | 1575 | Defined, zero callers repo-wide |
| `lookup_default_home_company(instance)` | 1589 | Defined, zero callers repo-wide |

Sibling helpers `lookup_default_employee_labor_type` (line 1603) and
`lookup_default_organization` (line 1615) **are** used by
`build_vp_employee_create_body_from_qbo` (lines 1793, 1801). The
two unused ones appear to be vestigial from an earlier shape of the
create body that included `EmployeeCompany` and `HomeCompany` fields.

### Suggested fix

Check `doc/CFG_MIGRATION.md` first — those `CFG_DefaultEmployeeCompany`
and `CFG_DefaultHomeCompany` keys may be staged for a future field
addition. If they are: keep the helpers but add a
`# WIRED FOR <ticket>` comment. If not: delete both helpers and the
corresponding rows from `CFG_MIGRATION.md`.

---

## D3. Shadowed `rail_config.py` defaults

**File**: `mapping_sync/rail_config.py` declares 4 constants that the
only consumer (`mapping_sync/instances/trial.py`) immediately
overrides:

| Constant | rail_config value | trial.py override |
|---|---|---|
| `environment` (line 7) | `'production'` | `'pre-production'` (trial.py:14) |
| `region` (line 8) | `'us-east-1'` | `'us-east-1'` (trial.py:13) — same value |
| `mapping_population_schedule` (line 17) | `"0 3 * * *"` | `"0 */23 * * *"` (trial.py:29) |
| `tenant_email` (line 22) | `'VantagePointQuickBooksIntegration@deltek.com'` | `'{{ var.value.vp_quickbooks_trial_email }}'` (trial.py:24) |

`trial.py` only imports `execution_timeout_days`,
`max_active_runs_master`, `max_active_runs_child`, `internal_logs_email`,
`alert_email` from `rail_config` — so the four shadowed values are
never read. Compare with `vendor_sync/` which has `qa.py` / `dev.py` /
`devops.py` instance files but no `rail_config.py` at all.

### Suggested fix

Two paths — pick one:

- **A.** Delete `environment`, `region`, `mapping_population_schedule`,
  `tenant_email` from `rail_config.py`. They're not inheritable because
  no instance file imports them. Keep the 5 constants that ARE
  imported.
- **B.** Add the missing `qa.py` / `dev.py` / `devops.py` files (matching
  vendor_sync) that inherit the rail_config defaults via `import *`-style
  inheritance. This is the larger change.

A is the immediate cleanup; B is the right answer if mapping_sync is
about to promote to multiple environments. Default to A until B is
needed.

---

## D4. Transaction-tracking placeholders

**Files**:

- `mapping_sync/transaction_tracking_dag.py` (119 lines)
- `mapping_sync/transaction_tracking/__init__.py` (15 lines)
- `mapping_sync/transaction_tracking/sales_invoices.py` (35 lines)
- `mapping_sync/transaction_tracking/purchase_invoices.py` (24 lines)
- `mapping_sync/transaction_tracking/employee_expenses.py` (25 lines)

All three `populate_outstanding_*` functions are no-op stubs that log a
warning and return 0. The DAG `transaction_tracking_dag.py` wires them
into the dispatcher chain (between `trigger_map_bank_code` and
`trigger_validate_mappings`). The dispatcher triggers this DAG on every
customer run, producing 3 useless `WARNING` log lines.

The placeholders ARE labeled as such in their docstrings, so this isn't
strictly dead code — but the analysis in
[`LOOKUP_TABLE_FLOWS.md`](LOOKUP_TABLE_FLOWS.md) concludes the
outstanding-tracking tables don't belong in `mapping_sync` at all —
they're transactional state managed by future GL-sync DAGs, not
one-shot mapping init.

### Suggested fix

If you accept the LOOKUP_TABLE_FLOWS recommendation:

1. Delete `mapping_sync/transaction_tracking/` subpackage (4 files).
2. Delete `mapping_sync/transaction_tracking_dag.py`.
3. From `mapping_sync/dispatcher_dag.py`: remove
   `trigger_transaction_tracking` (~stage 5 block) and
   `gather_transaction_tracking_error`; remove
   `gather_transaction_tracking_error` from the `combine_errors`
   sources list and from the `[...]` >> `combine_errors` chain.
4. From `mapping_sync/__init__.py`: remove the 3 re-exports.

When the real transaction_sync implementation lands, it goes in a
sibling `transaction_sync/` package — not back here.

Net delete: ~220 lines + 3 unnecessary warning log lines per
dispatcher run.

---

## D5. `_table_exists` defensive helper

**File**: `mapping_sync/utils/python_callable_method.py:3218`

Called by the four `validate_map_*` functions (lines ~3226, 3283,
3342, 3401) to short-circuit when the target table doesn't exist. By
the time validators run (Phase 5), the dispatcher's
`init_mapping_collections` (Stage 0.5) has guaranteed-created every
table. So `_table_exists` returns `True` 100% of the time.

The check costs one SQL `SELECT name FROM sqlite_master WHERE ...`
per validator (~negligible CPU, but adds a small amount of code
surface).

### Suggested fix

Drop `_table_exists` and its four call sites. The
`count_collection_rows` helper at line 492 already gracefully handles
`no such table` via exception catching (lines 521-522); if you want the
validators to share that defensive behaviour, the cleaner move is to
have validators consume `count_collection_rows`-shaped queries
directly. Otherwise just remove the always-true guard.

Lower priority — does no harm.

---

# Performance opportunities — detailed

## P1. Parallelize account + tax + bank_code after employee

**Files**: `mapping_sync/dispatcher_dag.py`

Current chain (lines ~252–319):

```
firm → employee → account → tax → bank_code → tx_tracking → validate
```

The `firm → employee` ordering IS required (employee mapping resolves
QBO Vendor IDs against the firm map at line ~1834). The next three
mapping steps have no data dependency on each other and don't depend
on `employee` either — they only need `init_mapping_collections` to
have created the tables.

### Suggested change

After `trigger_map_employee`, fan out to `trigger_map_account_code`,
`trigger_map_tax_code`, `trigger_map_bank_code` in parallel; converge
into `trigger_transaction_tracking`:

```
firm → employee ─┬→ account ──┐
                 ├→ tax ──────┤→ tx_tracking → validate
                 └→ bank_code ┘
```

### Concurrency safety

Each of the three children writes to a **different table** inside the
same `collections.db.gz`. The S3 collection artifact is shared, so two
parallel uploads will race on the ETag — but
`get_or_create_s3_collection_artifact`'s conditional-PutObject
(`IfMatch=<etag>`) detects this and raises
`S3CollectionConcurrencyError`. The remedy documented on that
exception (`s3_collection.py` line ~221) is **retry the task** —
mutations are deterministic given the operator inputs, so replay-on-
retry produces the same result against the newer state.

### Implementation notes

- Set `retries=1` (or `retries=2`) on `trigger_map_account_code`,
  `trigger_map_tax_code`, `trigger_map_bank_code` so an unlucky
  ETag race gets one auto-retry against the newer artifact.
- Each `sync_qbo_*_to_vp` helper already opens the artifact in a
  single download → modify → upload cycle, so the contention window
  is brief.
- `count_collection_rows` / `is_table_populated` skip gates also
  round-trip the artifact (read-only-ish), but they don't upload, so
  they don't contend.

### Expected impact

For a tenant with N firms, M employees, A accounts, T tax codes, B
banks:

- Today: `T_firm + T_employee + T_account + T_tax + T_bank`
- After:  `T_firm + T_employee + max(T_account, T_tax, T_bank)`

Typical reduction: ~30% of post-employee wall time when `T_account ≈
T_tax`.

---

## P2. Bulk-fetch existing VP firms / employees / accounts by QBOID

**Files**: `mapping_sync/utils/python_callable_method.py`

Per-record VP API calls used for the "VP already has this entity" check:

- `_find_vp_firm_by_qbo_id` (line 1172) — 1 GET per QBO firm
- `_find_vp_employee_by_qbo_id` (line 1849) — 1 GET per QBO employee
- `_find_vp_account_by_qbo_id` (line 2284) — 1 GET per QBO account

For 200 QBO firms that's 200 sequential VP GETs (network-bound, ~50–
200ms each). Same shape for employees and accounts.

### Suggested change

At the top of each sync helper, do **one** bulk GET that pulls every
VP entity with a non-null QBOID and indexes them in memory:

```python
# Replaces per-record _find_vp_firm_by_qbo_id calls.
existing_vp_firms_by_qboid = {}
for vp_firm in VantagepointFirmOperator(
    task_id='_bulk_vp_firms_by_qboid',
    vp_conn_id=vp_conn_id,
    request_method='GET',
    filters='?filterHash[0][name]=QBOID&filterHash[0][operator]=isnotnull',
    pagination=True,
).execute(context):
    qboid = vp_firm.get('QBOID')
    if qboid:
        existing_vp_firms_by_qboid[str(qboid)] = vp_firm
```

Then `_find_vp_firm_by_qbo_id(qbo_id, is_vendor_flag)` becomes a
dict lookup with the `VendorInd` post-fetch filter the current code
already does.

### Validate before implementing

- VP's `filterHash` may not support `isnotnull`. Worst case: pull all
  active firms (`?statusInactive=N`) and filter `QBOID is not None`
  in Python. Still 1 paginated call vs N sequential.
- Confirm the bulk GET returns the same fields per record as the
  current per-record call (the per-record code consumes
  `.QBOID`, `.VendorInd`, `.ClientID`, `.Category` for the firm case).

### Expected impact

For tenants with 100+ firms / 100+ employees / 500+ accounts, this is
THE dominant time cost reduction. Cuts the firm/employee/account
syncs from N×(50–200ms) to 1×(seconds). Especially valuable when
combined with P1.

---

## P3. Collapse 4 validators into 1 single-download task

**Files**:

- `mapping_sync/validate_mappings_dag.py` (current: 4 parallel PythonOperators)
- `mapping_sync/utils/python_callable_method.py:3226` (validate_map_firm), 3283 (validate_map_employee), 3342 (validate_map_account_code), 3401 (validate_map_tax_code)

Each of the 4 `validate_map_*` functions calls
`open_mapping_collection(read_only=True)` which downloads + decompresses
the customer's `collections.db.gz` to a fresh temp dir. Running them
in parallel means **4× the same S3 GetObject + 4× gunzip** for the
same artifact.

### Suggested change

Replace the 4 `PythonOperator` tasks with **one** task that opens
the collection once and runs all 4 checks inside the same context
manager:

```python
def run_all_mapping_validations():
    with open_mapping_collection(read_only=True) as conn:
        cur = conn.cursor()
        return {
            'map_firm':         _validate_map_firm_with_cursor(cur),
            'map_employee':     _validate_map_employee_with_cursor(cur),
            'map_account_code': _validate_map_account_code_with_cursor(cur),
            'map_tax_code':     _validate_map_tax_code_with_cursor(cur),
        }
```

Then `summarize_mapping_validations` consumes that single dict
instead of `rail.result(...)`-ing each of the 4 tasks.

### Trade-off

Loses per-validator Airflow task granularity (4 tasks → 1). If
operators want to retry a single validator independently, keep the
parallel structure. Otherwise the consolidation is strictly better.

### Expected impact

Saves 3 redundant S3 GetObject calls + 3 gunzip ops per validate-run.
Wall-clock impact is small (downloads happen in parallel today and
the file is single-digit MB), but it's a measurable reduction in S3
GET cost and worker memory.

---

## P4. Share QBO Accounts fetch between `map_account_code` + `map_bank_code`

**Files**:

- `mapping_sync/map_account_code_dag.py:112` — `fetch_qbo_accounts` (all active accounts)
- `mapping_sync/map_bank_code_dag.py:118` — `fetch_qbo_bank_accounts` (active accounts WHERE AccountType='Bank')

The bank-typed accounts are a subset of the all-accounts result.
Today both DAGs issue independent QBO queries.

### Suggested change

If P1 lands and these two run in parallel, this optimization
disappears — they don't share an execution context.

If they stay sequential (current state OR if you opt out of P1 for
bank_code specifically), have `map_bank_code` reuse the
`fetch_qbo_accounts` XCom from `map_account_code` and filter to Bank
in-memory.

### Expected impact

Saves 1 QBO `/query` call per dispatcher run. Lower priority — only
relevant if the chain stays sequential.

---

## P5. Batch `mark_step_status` updates in dispatcher

**Files**:

- `mapping_sync/utils/python_callable_method.py:229` — `_update_mapping_state_status`
- `mapping_sync/utils/python_callable_method.py:406` — `mark_step_status` (called by each map_*_dag's `mark_*_step_complete` PythonOperator)

Every child DAG calls `mark_step_status(step, 'Complete')` at the end
of its successful path. That triggers one
`S3UpdateCollectionOperator.execute` per child — meaning a full
download + write + upload of `collections.db.gz` for a single-row
UPDATE. With 5 children that's 5 sequential S3 round-trips on the
success path.

### Suggested change

Defer step-status updates to the dispatcher. Each child's success
already flows through the existing gather → no-errors → success path.
Add a single dispatcher-level PythonOperator (before
`mark_all_steps_ready`, which already does a bulk SET on all rows)
that writes `Status='Complete'` for every step that's not in the
error gather:

```python
# Pseudo:
def mark_succeeded_steps_complete():
    errored_tables = {e['table'] for e in rail.result('combine_child_dag_errors') or []}
    succeeded_steps = [
        step for step, table_name, _seq in MAPPING_STEPS_ORDERED
        if table_name not in errored_tables
    ]
    S3UpdateCollectionOperator(
        ...
        query=f"UPDATE mapping_table_state SET Status='Complete' WHERE Step IN ({','.join('?'*len(succeeded_steps))})",
        query_params=succeeded_steps,
    ).execute(...)
```

Then drop the per-child `mark_*_step_complete` tasks.

### Trade-off

Today's per-child mark is robust to partial failures: if a child
succeeds but a later child fails, the early child's
`Status='Complete'` persists and the next dispatcher run uses it as
a skip gate. After this change, no step is marked Complete until ALL
children succeed.

If you want the old "each step marks itself" semantics (for partial-
progress observability), keep the per-child marks. Otherwise the
batched version is cleaner.

### Expected impact

Saves 4 S3 round-trips per dispatcher success path. Low impact on
wall-clock (each round-trip is hundreds of ms) but tidies up the
"why are we re-uploading 5 times" pattern.

---

## P6. Verify read-only path used by skip gates

**Files**:

- `mapping_sync/utils/python_callable_method.py:492` — `count_collection_rows` (uses `S3QueryCollectionOperator`)

`count_collection_rows` is called by the `check_*_populated`
PythonOperator at the top of every map_*_dag. It uses
`S3QueryCollectionOperator`, which internally goes through
`get_or_create_s3_collection_artifact` (the writable artifact).
Possibly the artifact context manager re-uploads even when no SQL
mutation happened — verify against
`rail.lib.s3_collection.get_or_create_s3_collection_artifact`'s
hash-based skip-on-exit logic.

### Suggested change

If `S3QueryCollectionOperator` does in fact re-upload (because the
context manager doesn't know the query was SELECT-only), switch the
skip gates to use the local `open_mapping_collection(read_only=True)`
context manager directly — which downloads + decompresses without
ever uploading.

Today's working code calls `is_table_populated(table_name)` →
`count_collection_rows(table_name)` → `S3QueryCollectionOperator(mode='single-row')`.
The fix would be to replace that with:

```python
with open_mapping_collection(read_only=True) as conn:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table_name}')
    return cur.fetchone()[0]
```

### Verify before acting

Read `get_or_create_s3_collection_artifact` to confirm whether the
no-mutation upload skip actually fires for SELECT-only sessions. If
it does, this is already optimal and there's no change needed.

### Expected impact

Saves one S3 PutObject per `check_*_populated` task per dispatcher
run (5 tasks × 1 PutObject each = 5 wasted PutObjects), IF the upload
isn't already being skipped. Low priority unless verification shows
the skip isn't firing.

---

# Iteration workflow

When picking up an item:

1. Re-verify the finding with a quick `grep` / `Read` (line numbers
   may have shifted).
2. Make the change in a focused commit; reference this doc + the item
   ID in the commit message (e.g. `MAP2-XXXX: D1 — trim config.py
   dead constants`).
3. Update the **Status** column in the backlog index above
   (pending → done, or pending → won't-do with a one-line rationale).
4. For perf items: capture before/after metrics in the commit body
   when feasible (S3 GET count, VP API call count, wall-clock on a
   reference tenant).
