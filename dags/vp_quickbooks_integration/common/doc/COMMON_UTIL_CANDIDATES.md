# Common utility candidates — repeated methods across vp_quickbooks_integration

**Status:** Analysis / inventory · **Date:** 2026-06-08
**Purpose:** Catalogue utility methods duplicated across the workflow folders so
they can be consolidated into `vp_quickbooks_integration/common/`. This is a
discovery list only — no code has been moved.

## Scope scanned

Every `utils/python_callable_method.py` plus mapping_sync's `utils/_*.py`.
Legend of integration → file (all paths under
`dags/vp_quickbooks_integration/`):

| Tag | File path |
|---|---|
| CS  | `customer_sync/utils/python_callable_method.py` |
| CSU | `customer_sync_upsert/utils/python_callable_method.py` |
| VS  | `vendor_sync/utils/python_callable_method.py` |
| ES  | `employee_sync/utils/python_callable_method.py` |
| ESU | `employee_sync_upsert/utils/python_callable_method.py` |
| TS  | `timesheets_sync/utils/python_callable_method.py` |
| IPS | `invoice_payment_sync/utils/python_callable_method.py` |
| BPS | `bill_payment_sync/utils/python_callable_method.py` |
| JES | `journal_entry_sync/utils/python_callable_method.py` |
| UTS | `unit_transaction_sync/utils/python_callable_method.py` |
| COA | `chart_of_accounts_sync/utils/python_callable_method.py` |
| MS  | `mapping_sync/utils/_shared.py` (re-exported via `mapping_sync/utils/python_callable_method.py`) |

## Summary — families, spread, proposed home

| # | Family | Workflows affected | Proposed `common` module |
|---|---|---|---|
| 1 | Watermark / sync-window timestamps | CS, IPS, BPS, TS, UTS, VS, ES, ESU, JES, COA (10) | `common/watermark.py` |
| 2 | Error capture + label formatting | all 12 | `common/errors.py` |
| 3 | VP response unwrapping | CSU, VS, ES, ESU, JES, COA (6) | `common/responses.py` |
| 4 | QBO response normalisation | MS, BPS, COA (+others entity-wrapped) | `common/responses.py` |
| 5 | Variable JSON read/write (+ map read/write) | CSU, VS, ES, ESU, JES, COA (6) | `common/variables.py` |
| 6 | `_filter_none` / `_filter_none_and_empty` | CSU, VS, IPS, BPS, MS, ES (6) | `common/dicts.py` |
| 7 | dag_run.conf accessors | CS, IPS, BPS, JES, COA (5) | `common/context.py` |
| 8 | Disabled/enabled integration flag | CS, IPS, BPS, TS, UTS (5) | `common/flags.py` |
| 9 | Router dagrun-id collection | CS, CSU, VS, ES (4) | `common/orchestration.py` |
| 10 | Child-DAG conf builder | CS, CSU, VS, ES, MS, COA (6) | `common/orchestration.py` |
| 11 | Date / boolean / status coercion | ES, TS, UTS, JES, VS, CSU, MS (7) | `common/coerce.py` |
| 12 | Feature-capability gate | CSU, TS (2) | `common/flags.py` |
| 13 | S3 collection access | BPS, MS (2) | `common/collections.py` |
| 14 | CFG-then-Variable resolver | MS (1, promote) | `common/variables.py` |

---

## 1. Watermark / sync-window timestamps  *(highest duplication — 10 workflows)*

Same logic in two naming shapes. Reconcile to one parameterised API
(`prepare_sync_timestamps(instance, template, initial_sync_time)` etc.).

**Variant B — parameterised (verbatim duplicates):** `sanitize_customer_id`,
`build_watermark_variable_key(template, instance, customer_id)`, `utc_now_iso`,
`prepare_sync_timestamps(instance, template, initial_sync_time)`,
`update_last_sync_time(instance, template)`
- VS, ES, ESU, JES, COA

**Variant A1 — `_method` suffixed:** `_customer_id_from_conf`, `_watermark_key`,
`_now_iso`, `_validate_qbo_timestamp`, `_parse_iso_utc`,
`prepare_*_sync_timestamps_method`, `update_*_last_sync_times_method`
- CS  (`prepare_customer_sync_timestamps_method`, `update_customer_last_sync_times_method`; no `_parse_iso_utc`)
- IPS (`prepare_payment_sync_timestamps_method`, `update_payment_last_sync_times_method`)
- BPS (same as IPS)

**Variant A2 — `_`-prefixed:** `_sanitize_customer_id`, `_watermark_variable_key`,
`_utc_now_iso`, `prepare_sync_timestamps_method`, `update_last_sync_time_method`
- TS, UTS

Shared sub-helpers worth a single home: `_now_iso`/`utc_now_iso`/`_utc_now_iso`,
`_parse_iso_utc` (IPS, BPS), `_validate_qbo_timestamp` (CS, IPS, BPS).

## 2. Error capture + label formatting  *(all 12)*

All return a dict and never raise; only the id/label fields differ.

- Router-level `capture_router_dag_error(id, label, fallback)` — CS, CSU, VS, ES
- `capture_create_error` / `capture_update_error` — CSU, VS, ES
- `capture_processor_error(...)` — ESU, JES, COA
- `capture_create_dag_error(post_seq, period, fallback)` — TS, UTS
- Leaf `capture_<entity>_dag_error` — CS (`capture_customer_dag_error`), IPS (`capture_payment_dag_error`), BPS (`capture_bill_payment_dag_error`)
- `capture_dag_error(table, customer_id, error)` — MS
- Label formatters `_format_<entity>_label(id, name)` — CSU (`_format_firm_label`), VS (`_format_vendor_label`), ES & ESU (`_format_employee_label`)

→ One `capture_dag_error(entity_id, label, fallback_error)` + `format_entity_label(id, name)`.

## 3. VP response unwrapping  *(6)*

Normalise a Vantagepoint operator response envelope (bare list / `rows` /
`Body` / `body` / `array` / `data`) to a list. Same intent, 7 names:
- `_unwrap_vp_response(raw, strict=False)` — JES, COA, ESU
- `_unwrap_list_response(raw, list_keys)` — CSU
- `_unwrap_settings_response(raw)` — ES
- `_unwrap_contacts_response`, `_unwrap_address_response`, `_unwrap_veaccounting_response` — VS
- single-record unwrappers: `_fetched_record` (TS), `_fetched_vp_employee` (ESU), `_fetched_rows`/`_fetched_projects` (UTS)

→ `unwrap_vp_response(raw, list_keys=None, strict=False)` + `first_or_empty(raw)`.

## 4. QBO response normalisation

Normalise a QuickBooks*Operator response (`{success, data}` / raw list /
`{QueryResponse: {Entity: [...]}}`) to a list of records:
- `_extract_qbo_records(rail_result)` — MS (canonical, handles all three shapes)
- BPS reads `result.get('data')`; COA `extract_account_list_method` does similar inline

→ Promote `_extract_qbo_records` + `_extract_qbo_entity_id` + `_extract_vp_client_id` (all in MS `_shared.py`).

## 5. Variable JSON read/write (+ map read/write)  *(6)*

Generic JSON-backed Airflow Variable accessors under two names:
- `_read_lookup_variable(key, default=None)` — CSU, VS, ES
- `_read_json_variable(key)` — JES, COA

Entity-keyed map wrappers built on the above (the read/write primitive is
generic; the specific Variable key + row shape stays per-integration):
- `_read_firm_map` / `_write_firm_map` — CSU, VS (JES `_read_firm_map`)
- `_read_employee_map` / `_write_employee_map` — ES, ESU
- `_read_account_map` / `_write_account_map` — COA (JES `_read_account_map`)

→ `read_json_variable(key, default)` / `write_json_variable(key, value)` in common; keep the typed map helpers local.

## 6. `_filter_none` / `_filter_none_and_empty`  *(6)*

Drop `None` (and optionally empty-string) keys before sending a request body:
- `_filter_none(body)` — CSU, VS, IPS, BPS, MS
- `_filter_none_and_empty(body)` — ES (variant that also drops `''`)

→ `filter_none(body, drop_empty=False)`.

## 7. dag_run.conf accessors  *(5)*

- `_get_conf()` — IPS, BPS
- `_conf()` / `_conf_value(key, default='')` — JES, COA
- `_customer_id_from_conf()` — CS, IPS, BPS
- `_payload_from_conf()` — CS

→ `get_conf()`, `conf_value(key, default='')`, `customer_id_from_conf()`.

## 8. Disabled / enabled integration flag  *(5)*

`is_integration_enabled_method(instance)` — two-level check (per-tenant
`CFG_Disable<X>Integration_{customer}_{instance}` then instance-level fallback):
- CS, IPS, BPS, TS, UTS

→ `is_integration_enabled(instance, cfg_name)` (parameterise the CFG flag name).

## 9. Router dagrun-id collection  *(4)*

`collect_triggered_dagrun_ids()` — collect dag-run ids from whichever of the
create/update trigger tasks fired: CS, CSU, VS, ES.

## 10. Child-DAG conf builder  *(6)*

Forward `connections` / `customerId` / `integrationType` (+ operation/region)
into a child DAG's conf:
- `build_customer_conf(operation_type)` — CS, CSU
- `build_vendor_conf(operation_type)` — VS
- `build_employee_conf(operation_type)` — ES
- `build_child_dag_conf()` — MS
- `build_processor_dag_conf(item)` — COA

→ `build_child_conf(extra=None)` forwarding the standard keys; callers add entity bits.

## 11. Date / boolean / status coercion  *(7)*

- Date → `YYYY-MM-DD`: `format_date_to_yyyy_mm_dd` (ES), `_format_qbo_date` (TS, UTS), `_normalize_txn_date` (JES), `_format_vp_date` (MS `_employee_sync`)
- Boolean → `'Y'/'N'`: `_yes_no` (VS)
- VP `A/I` ↔ QBO `Active` bool: `_qbo_status_to_vp` (VS), `_vp_status_to_qbo_active` (CSU), `_qbo_status_to_vp_employee` (ES), `_qbo_status_to_vp_status` (MS `_firm_sync`)
- String coercion `_s(value)` (COA)

→ `to_qbo_date(value)`, `yes_no(value)`, `vp_status_from_active(active)` / `active_from_vp_status(status)`.

## 12. Feature-capability gate  *(2)*

`_qbo_capability_enabled(capability)` — per-tenant + per-instance feature flag
(multi_currency / sales_tax / time_tracking): CSU, TS.

## 13. S3 collection access  *(2)*

Read/write the mapping_sync-owned S3 collections:
- BPS: `_collection_integration(context)`, `_collection_single_row(query, params)`, `_collection_rows(table, columns, where, params)`, `_collection_update(name, query, params)`
- MS: `open_mapping_collection(read_only=False)`, `_resolve_s3_locator(context)`, `count_collection_rows(table)`, `is_table_populated(table)`

→ `common/collections.py` (sits naturally next to the already-moved `common/tables.py`). Note locator differs: BPS pins `integration_type='mapping_sync'`; MS reads it from conf — parameterise.

## 14. CFG-then-Variable resolver  *(promote from MS)*

`_resolve_cfg_then_variable(cfg_key, variable_name)` — MS `_shared.py`. The
`lookup_default_*` helpers in CS/VS/ES/COA are the same pattern open-coded.

---

## What stays integration-specific (do NOT move)

Entity body builders and field mappings: `build_create_*_body`,
`build_update_*_body`, `build_*_filter`, `compute_*_lines`, `extract_*_list`,
`_addr_block` / `_qbo_*_inputs`, `resolve_*`, the four mapping_sync sync engines
(`sync_qbo_firms/employees/accounts/tax_codes_to_vp`), `_validate.py`, and the
typed `*_map` row schemas. These encode QBO↔VP business logic per entity.

## Suggested sequencing (when consolidation is approved)

1. `watermark.py` (#1) — biggest win, lowest risk; reconcile the two variants into one API, repoint all 10.
2. `errors.py` (#2), `responses.py` (#3, #4), `dicts.py` (#6), `context.py` (#7) — small pure helpers.
3. `variables.py` (#5, #14), `flags.py` (#8, #12), `orchestration.py` (#9, #10), `coerce.py` (#11).
4. `collections.py` (#13) — alongside `common/tables.py`.

Each step: move to `common/<module>.py`, repoint imports, verify py_compile
+ dagbag parse. rail-library is unaffected (no rail symbols), so no Airflow
restart needed for these moves.
