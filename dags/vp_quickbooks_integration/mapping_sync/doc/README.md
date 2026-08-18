# Documentation — `mapping_sync/`

This directory holds all Markdown documentation for the `mapping_sync`
sub-package of the VantagePoint ↔ QuickBooks Airflow integration. The
package root keeps only `__init__.py`; every `.md` file lives in here.

> **Convention** — new `.md` files for `mapping_sync/` go in
> `mapping_sync/doc/`. Other packages (`vendor_sync/`, `employee_sync/`)
> follow the same per-package pattern when they grow docs of their own.
>
> The Workato source-of-truth tree at
> `integration_vantagepoint_quickbooks/docs/` is a separate, already-
> organized hierarchy and stays as-is.

## Contents

### Operational fix logs

Running per-table fix logs for the `mapping_sync` DAGs. Each entry
follows the same shape: **Symptom → Root cause → Fix → Workato
reference → Code touchpoints**. Cross-referenced from code comments
(`# See MAP_FIRM_SYNC_FIX_LOG.md #11`).

- [`MAP_FIRM_SYNC_FIX_LOG.md`](MAP_FIRM_SYNC_FIX_LOG.md)
- [`MAP_EMPLOYEE_SYNC_FIX_LOG.md`](MAP_EMPLOYEE_SYNC_FIX_LOG.md)
- [`MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md`](MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md)
- [`MAP_TAX_CODE_SYNC_FIX_LOG.md`](MAP_TAX_CODE_SYNC_FIX_LOG.md)

### Configuration / migration

- [`CFG_MIGRATION.md`](CFG_MIGRATION.md) — middleware `CFG_*` payload
  → Airflow Variable migration table; which keys are wired, which are
  deferred, and which `Variable.get` sites are intentionally not
  migrated.

### Lookup-table flows (Workato → Airflow)

- [`LOOKUP_TABLE_FLOWS.md`](LOOKUP_TABLE_FLOWS.md) — flow + schema
  reference for the configuration tables (`bank_code_map`,
  `invoice_section_code`, `pay_terms`) and transaction-tracking tables
  (`outstanding_employee_expenses`, `outstanding_purchase_invoices`,
  `outstanding_sales_invoices`). Note: `bank_code_map` is no longer
  populated by mapping_sync — bank-code resolution is a transaction-time
  concern handled by `invoice_payment_sync` (see §1 of that doc).

### Cleanup + performance backlog

- [`MAPPING_SYNC_CLEANUP_AND_PERF.md`](MAPPING_SYNC_CLEANUP_AND_PERF.md)
  — dead-code findings (5 items: D1–D5) and perf-optimization
  opportunities (6 items: P1–P6) for `mapping_sync/`. Iteration
  reference; each item is independently actionable with a Status
  column for tracking progress.

### Cross-repo fix notes

- [`QUICKBOOKS_BATCH_TASK_OPERATOR_FIX.md`](QUICKBOOKS_BATCH_TASK_OPERATOR_FIX.md)
  — RAIL-side kwarg-collision fix in the `QuickBooks*Operator`
  subclasses, surfaced while landing the `BatchTaskRunOperator` wrap
  on the 5 `map_*` child DAGs. Documents the root cause
  (`_BaseOperator__init_kwargs` leak via Airflow's `apply_defaults`),
  the 4-line `kwargs.pop()` patch applied to each subclass, the 5
  files patched in this rollout vs the 5 deferred to a follow-up
  RAIL PR, and the verification recipe (restart scheduler + worker,
  then run with batch on and off).

### Historical cleanup notes

Kept for context on the package restructuring that landed earlier.
Safe to delete if you don't want them in the repo history (`git log`
covers the same ground).

- [`CLEANUP_ANALYSIS.md`](CLEANUP_ANALYSIS.md)
- [`FINAL_CLEANUP_SUMMARY.md`](FINAL_CLEANUP_SUMMARY.md)

### Prompts / agent specs

- [`vantagepoint-integration-builder-prompt.md`](vantagepoint-integration-builder-prompt.md)
  — agent prompt used when generating new VP-side integrations.

## Cross-references from code

Code comments under `mapping_sync/` reference these documents by bare
filename, e.g.:

```python
# See MAP_EMPLOYEE_SYNC_FIX_LOG.md #7 and the firm analog in
# `_find_vp_firm_by_qbo_id`.
```

The bare-filename convention is preserved across the move — the names
are unique repo-wide, so a reader can still locate them via `find` /
their editor's open-file dialog. The canonical location is now `doc/`.
