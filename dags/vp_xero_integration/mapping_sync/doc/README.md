# Documentation — `mapping_sync/`

This directory holds all Markdown documentation for the `mapping_sync`
sub-package of the VantagePoint ↔ Xero Airflow integration. The package root
keeps only the DAG/engine modules; every `.md` file lives in here.

> **Convention** — new `.md` files for `mapping_sync/` go in
> `mapping_sync/doc/` (never at the package root). Mirrors the QuickBooks
> `vp_quickbooks_integration/mapping_sync/doc/` layout.
>
> The Workato source-of-truth tree at `integration_vantagepoint_xero/code/` is a
> separate, already-organized hierarchy and stays as-is. The reverse-engineering
> parity docs that drove this build live under
> `vp_xero_integration/aidlc-docs/inception/reverse-engineering/xero-mapping-sync/`
> (docs 00–08).

## What this package does

`mapping_sync` performs the **Initial Mapping Sync** from Xero into Vantagepoint:
it builds the firm / chart-of-accounts / tax-code cross-reference tables a
tenant needs before any GL transaction flow can run. It is a one-shot
per-customer setup step (gated by a per-customer init Variable), not an ongoing
sync.

```
main_dag (scheduled)            → fetch enabled customers, trigger dispatcher per customer
  └─ dispatcher_dag             → init gate → init_mapping_collections → premapping
                                   → map_firm → map_account_code → map_tax_code
                                   → validate_mappings → gather errors → ready / init-complete
       ├─ map_firm_dag          → Xero Contacts → VP Firms          (utils/_firm_sync.py)
       ├─ map_account_code_dag  → Xero Accounts → VP Chart of Accts  (utils/_account_sync.py)
       ├─ map_tax_code_dag      → Xero TaxRates → VP Tax Codes       (utils/_tax_code_sync.py)
       └─ validate_mappings_dag → Phase-5 referential checks         (utils/_validate.py)
```

Source data is read through **RAIL Xero operators** (`XeroContactOperator`,
`XeroAccountOperator`, `XeroTaxRateOperator` — all paginate); VP writes go
through the Vantagepoint operators. No custom operators (CLAUDE.md mandate).

## Contents

### Operational fix logs

Per-table logs recording where the Airflow port **deliberately diverges from the
Workato recipe to fix a Workato bug** (decision Q9). Each entry follows the
shape **Symptom → Root cause → Fix → Workato reference → Code touchpoints**.

- [`MAP_FIRM_SYNC_FIX_LOG.md`](MAP_FIRM_SYNC_FIX_LOG.md)
- [`MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md`](MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md)
- [`MAP_TAX_CODE_SYNC_FIX_LOG.md`](MAP_TAX_CODE_SYNC_FIX_LOG.md)

### Lookup-table flows (Workato → Airflow)

- [`LOOKUP_TABLE_FLOWS.md`](LOOKUP_TABLE_FLOWS.md) — flow + schema reference for
  the non-`map_firm/account/tax` collections: the `mapping_table_state` machine,
  the **seeded** `map_account_type` reference table, and the sibling collections
  created up front (`map_employee`, `map_bank_code`, `map_currency_code`,
  `outstanding_employee_expenses`, `outstanding_purchase_invoices`).

## Single source of truth

Table names, column lists, UNIQUE keys, and the `map_account_type` seed rows live
in [`vp_xero_integration/common/tables.py`](../../common/tables.py). The
dispatcher's `init_mapping_collections` creates every collection from those
constants in one S3 round-trip.
