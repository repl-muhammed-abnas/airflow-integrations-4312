# 00 — Architecture Parity: QBO `mapping_sync` template → Xero port

This document describes the existing **QuickBooks** Airflow reference implementation
(`airflow-integrations/dags/vp_quickbooks_integration/mapping_sync`) that the Xero
(`vp_xero_integration`) build must mirror, then lists exactly what changes for Xero.

---

## 1. Target folder layout for `vp_xero_integration`

Mirror the QBO package one-for-one:

```
airflow-integrations/dags/vp_xero_integration/
├── common/
│   ├── config.py                  # shared defaults (region, timeouts, emails)
│   ├── tables.py                  # SINGLE SOURCE OF TRUTH: table names, columns, UNIQUE keys, static maps
│   ├── python_callable_method.py  # generic S3 collection read/write helpers
│   ├── main_dag.py                # placeholder DAG (folder-shape anchor)
│   └── instances/{dev,qa,devops,trial}.py
└── mapping_sync/
    ├── main_dag.py                # SCHEDULED entry: token → list customers → trigger dispatcher per customer
    ├── dispatcher_dag.py          # per-customer orchestrator (init → premapping → child DAGs → gather → ready)
    ├── map_firm_dag.py            # child DAG (← synch_firms)
    ├── map_account_code_dag.py    # child DAG (← synch_accounts)
    ├── map_tax_code_dag.py        # child DAG (← sync_tax_codes)
    ├── map_employee_dag.py        # child DAG (← synch_employees — NOT in this analysis scope, but exists in 014-501)
    ├── validate_mappings_dag.py   # phase-5 validation
    ├── config.py                  # IntegrationConfig (S3 names, conn-ids, dag-id builder, CFG resolver)
    ├── instances/{dev,qa,devops,trial}.py
    ├── utils/
    │   ├── python_callable_method.py   # re-export shim (DAGs import from here)
    │   ├── _shared.py                  # S3 collection access, state lifecycle, conf builder, error capture
    │   ├── _firm_sync.py               # firm engine + body builders
    │   ├── _account_sync.py            # account engine + compile SQL
    │   ├── _tax_code_sync.py           # tax engine + flatten + compile SQL
    │   ├── _employee_sync.py           # employee engine
    │   └── _validate.py                # per-table validators
    └── doc/
        ├── README.md
        ├── LOOKUP_TABLE_FLOWS.md       # (see §6 for the section template to reproduce)
        └── MAP_*_SYNC_FIX_LOG.md       # per-table fix logs (Symptom → Root cause → Fix → Workato ref → Code touchpoints)
```

> **Scope note:** This analysis covers **firms, accounts, tax codes**. The 014-501 package also has `synch_employees` (firms recipe references it) — out of scope here but should be a sibling child DAG, same pattern.

---

## 2. DAG topology (identical to QBO)

```
main_dag (scheduled, per instance)
  └─ get_middleware_auth_token (SimpleHttpOperator, POST /api/v1/oauth/token)
  └─ fetch_customers_by_integration (SimpleHttpOperator, GET /api/v1/integrations)
  └─ TriggerDagRunForEachItemOperator  → one dispatcher run per enabled customer
       └─ dispatcher_dag (schedule_interval=None)
            ├─ is_mapping_init_already_done (IfOperator, reads Variable vp_xero_mapping_init_{customerId}_{instance})
            │     ├─ true  → skip_mapping_init
            │     └─ false → init_mapping_collections (S3CreateMultiTableCollectionOperator)
            ├─ apply_premapping_state (PythonOperator; reads CFG_UpgradeDataSync)
            ├─ trigger_map_firm          (TriggerDagRunOperator, wait_for_completion=True)
            ├─ trigger_map_employee
            ├─ trigger_map_account_code
            ├─ trigger_map_tax_code
            ├─ trigger_validate_mappings
            ├─ gather child errors (GatherResultsFromDagRunsOperator × children)
            ├─ combine_child_dag_errors (PythonOperator, trigger_rule=none_skipped)
            ├─ has_sync_errors (IfOperator)
            │     ├─ yes → fail_mapping_sync (FailOperator)
            │     └─ no  → update_last_run_time → mark_all_steps_ready → mark_mapping_init_complete
            └─ post_dag_run_details (PostDagRunDetailsToMiddlewareApiOperator, trigger_rule=all_done)
```

**Strict child ordering** (enforced by `>>` chaining): `firm → employee → account_code → tax_code → validate`. Firm first (root entity); validation last.

### Per-child DAG shape (every `map_*_dag.py`)
1. `ViewDagRunConfOperator` (log conf).
2. Batch gate: `can_run_batch_task` (IfOperator, Variable `vp_xero_mapping_sync_can_run_batch`, default `'true'`) → `BatchTaskRunOperator` (runs the task range in one process) **or** the legacy per-task path.
3. Skip gates: `check_<x>_step_complete` (`check_step_status` → reads `mapping_table_state.Status == 'Complete'`) **OR** `check_<x>_populated` (`is_table_populated`).
4. Population: the `sync_<x>` python_callable engine.
5. `mark_<x>_step_complete` (`mark_step_status(step,'Complete')`).
6. `catch_<x>_dag_error` (PythonOperator, `trigger_rule='one_failed'`, calls `capture_dag_error`, **never raises** → child run stays SUCCESS, dispatcher reads the captured error).

---

## 3. State machine — `mapping_table_state` (Workato parity)

| Column | Meaning |
| --- | --- |
| `Step` | canonical step name: `Map Firms` / `Map Employees` / `Map Accounts` / `Map Tax Codes` |
| `DagId` | child DAG id (Workato col `Recipe`) |
| `TableName` | canonical S3 collection for the step (`map_firm`, …) |
| `Status` | `'' \| 'Complete' \| 'Error' \| 'Ready'` |
| `Messages` | free-text error detail |
| `Sequence` | `'10' / '20' / '30' / '40'` |

- **Seed** at init (`seed_mapping_state_rows`) from `MAPPING_STEPS_ORDERED`.
- **`apply_premapping_state`** reads `CFG_UpgradeDataSync`: `false` → set all `Complete` (children skip); `true` → set `''` (children run). Content-aware override: if `false` but tables empty → flip to `''` (fresh customer still syncs).
- Each child success → `mark_step_status('Complete')`; validation failure → `'Error'`; final dispatcher success → bulk `'Ready'`.

---

## 4. RAIL operators used (no custom operators — CLAUDE.md mandate)

| Category | Operators (accessed `rail.<Name>`) |
| --- | --- |
| DAG factory | `create_airflow_dag`, `for_each_instance` |
| Control flow | `IfOperator`, `PythonOperator`, `EmptyOperator`, `Label`, `FailOperator`, `ViewDagRunConfOperator` |
| Orchestration | `TriggerDagRunOperator`, `TriggerDagRunForEachItemOperator`, `BatchTaskRunOperator`, `GatherResultsFromDagRunsOperator` |
| S3 collections | `S3CreateMultiTableCollectionOperator`, `S3QueryCollectionOperator`, `S3UpdateCollectionOperator` |
| Run-local collections | `CreateCollectionOperator`, `QueryCollectionOperator` |
| HTTP / middleware | `SimpleHttpOperator`, `PostDagRunDetailsToMiddlewareApiOperator` |
| Vantagepoint (TARGET — unchanged for Xero) | `VantagepointFirmOperator`, `VantagepointEmployeeOperator`, `VantagepointChartOfAccountsOperator`, `VantagepointTaxCodesOperator`, `VantagepointSystemFormatsOperator`, `VantagepointAPIOperator`, `VantagepointCustomOperator`, `organization`/`codetable` via custom ops |
| QuickBooks (SOURCE — REPLACE with Xero) | `QuickBooksCustomerOperator`, `QuickBooksVendorOperator`, `QuickBooksEmployeeOperator`, `QuickBooksAccountOperator`, `QuickBooksTaxCodeOperator`, `QuickBooksTaxRateOperator` |
| Low-level libs in callables | `rail.lib.s3_collection.*`, `rail.lib.collection.*`, `rail.result('task_id')`, `rail.get_current_context()` |

> **ACTION FOR XERO:** confirm RAIL ships Xero operators (e.g. `XeroContactOperator`, `XeroAccountOperator`, `XeroTaxRateOperator`) and a Xero hook/connection. The `replicon-airflow-library` already contains Xero support (`airflow-integrations/dags/xero/` connector DAGs exist). If a needed Xero operator is missing, **raise it for RAIL** — do not write a custom operator.

---

## 5. Helper engine pattern (`_*_sync.py`) — reproduce verbatim

Every engine follows the same shape:
1. `context = rail.get_current_context()`; `log = context['task_instance'].log`.
2. Normalise source response (`_extract_*_records(rail.result('fetch_...'))`).
3. Resolve conn-ids via `IntegrationConfig.get_conn_ids(context)`.
4. **Single S3 download → mutate all → single upload**: `with get_or_create_s3_collection_artifact(...) as artifact: with sqlite3.connect(artifact.local_filename) as conn:` — never per-record S3 round-trips.
5. `_load_existing_map_*_index(cur)` → in-memory dict keyed by the natural key.
6. Bulk-load VP records once (avoid N GETs).
7. Per-record `try/except` accumulating into `summary['errors']`; `conn.commit()`; raise `RuntimeError` at end if any error (so `catch_*` fires).

Upsert: `INSERT OR REPLACE` against the UNIQUE key (firm, tax) **or** SELECT-then-UPDATE/INSERT where multiple rows per source id are allowed (accounts).

---

## 6. `config.py` / `IntegrationConfig` + instance layering

`IntegrationConfig` constants to set for Xero:

| QBO value | Xero value |
| --- | --- |
| `S3_INTEGRATION_NAME = 'vp_quickbooks_integration'` | `'vp_xero_integration'` |
| `DAG_ID_PREFIX = 'vp_qbo_mapping_sync'` | `'vp_xero_mapping_sync'` |
| `CAN_RUN_BATCH_VARIABLE_NAME = 'vp_qbo_mapping_sync_can_run_batch'` | `'vp_xero_mapping_sync_can_run_batch'` |
| init Variable `vp_qbo_mapping_init_{customerId}_{instance}` | `vp_xero_mapping_init_{...}` |
| `QUICKBOOKS_CONN_ID = 'quickbooks_default'`; conf key `connections.intuit` | `XERO_CONN_ID = 'xero_default'`; conf key `connections.xero` |
| `VANTAGEPOINT_CONN_ID = 'vantagepoint_default'` | *unchanged* |

- `get_cfg(context, key, default)` reads `dag_run.conf['config'][key]`, with per-instance Airflow-Variable fallback (`_resolve_cfg_then_variable`).
- Instances (`dev/qa/devops/trial`) import shared defaults and add: `instance`, `region`, `environment`, `company_key`, `middleware_conn_id = f"middleware_conn_{instance}"`, `default_region='US'`, `tenant_email`, `mapping_population_schedule = "0 3 * * *"`.
- Only `main_dag` is scheduled; dispatcher + children are `schedule_interval=None`.
- All DAGs: `multi_tenant=True`, `company_key=config.company_key`.

---

## 7. `doc/LOOKUP_TABLE_FLOWS.md` section template (reproduce for Xero)

The QBO doc documents the **non-`map_*` tables** (state/tracking/static). Reproduce this outline:

```
# Lookup-Table Flows — Workato source-of-truth + Airflow implementation guide
   (intro + scope table: Table | Category | Airflow status)

## N. `<table>` — <category>
   ### Workato source        (recipe paths + lifecycle: seed / add / update / delete)
   ### Schema                (markdown table: col | Workato label | Airflow column)
   ### Airflow implementation (status: DONE / guidance / removed)

## Cross-cutting concerns
   ### Distributed locking
   ### UNIQUE constraints
   ### State-row plumbing (mapping_table_state)

## Summary table  (Table | Pop. in Workato | Pop. in Airflow | Where it belongs)
```

Companion `doc/README.md` indexes all docs; fix-logs follow **Symptom → Root cause → Fix → Workato reference → Code touchpoints**.

---

## 8. QBO → Xero change checklist (summary)

| Area | Change |
| --- | --- |
| Package / namespace | `vp_quickbooks_integration` → `vp_xero_integration`; dag-id prefix, S3 name, Variable prefixes, tags (`vantagepoint_xero`) |
| Source API operators | QuickBooks* operators → Xero* operators; `intuit_conn_id` → `xero_conn_id`; conf `connections.intuit` → `connections.xero` |
| VP target operators | **none** — VP is still the target |
| Field identifiers | `QBO*` columns (`QBOID`, `QBOCode`, `QBOName`, `QBOType`, `QBOCodeID`, `QBORateID`, …) → `Xero*` (`XeroID`, `XeroCode`, `XeroName`, `XeroType`, …) across `common/tables.py`, SQL, validators |
| Data model differences | Xero **Contacts** (IsCustomer/IsSupplier) not QBO Customer/Vendor; Xero **Accounts** (Code/Class/Type/AccountID); Xero **TaxRates with nested TaxComponents** (no separate TaxCode/TaxRate split — component fan-out) |
| Static maps | re-key `ACCOUNT_TYPE_MAP` and `PAY_TERMS_MAP` for Xero enums/terms |
| Matching keys | firm: by **Name** (Xero has no VP-stored id); account: AccountID/Code; tax: RateName+ComponentName |
| Validators | update `QBO*` identifiers + the AP-account business rule to Xero equivalents |

See per-recipe docs (01–03) for the detailed logic each child DAG must reproduce, and [04-lookup-tables.md](04-lookup-tables.md) for exact column definitions.
