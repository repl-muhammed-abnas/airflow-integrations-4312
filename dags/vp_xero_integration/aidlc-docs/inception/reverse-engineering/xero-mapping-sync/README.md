# Xero Mapping Sync — Workato → Airflow Parity Analysis

> **Purpose:** Reference documentation for re-implementing the Vantagepoint ↔ Xero **Initial Mapping Sync** (Workato package `014-501 PSA`) as an Airflow integration under `airflow-integrations/dags/vp_xero_integration`, mirroring the existing QuickBooks reference at `airflow-integrations/dags/vp_quickbooks_integration/mapping_sync`.
>
> These documents feed the **User Story** stage and the subsequent Airflow development. They capture *what the Workato recipes do* (parity) and *how the QBO Airflow port solved the same problem* (template), so the Xero port can be built by analogy.

## Scope analysed (in the order requested)

| # | Workato recipe (source) | Real logic lives in | Airflow target |
| --- | --- | --- | --- |
| 1 | `Mapping/Initial Synch/014_501_psa_synch_firms.recipe.json` | self-contained (12.3k lines) | `vp_xero_integration/mapping_sync/map_firm_dag.py` |
| 2 | `Mapping/Initial Synch/014_501_psa_synch_accounts.recipe.json` | orchestrator + worker `GL/014_501_psa_sync_accounts.recipe.json` | `.../map_account_code_dag.py` |
| 3 | `Mapping/Initial Synch/014_501_psa_synch_tax_codes.recipe.json` | thin async wrapper → `GL/014_501_psa_sync_tax_codes.recipe.json` (5.7k lines) | `.../map_tax_code_dag.py` |

## Document index

| Doc | Contents |
| --- | --- |
| [00-architecture-parity.md](00-architecture-parity.md) | The QBO `mapping_sync` reference architecture (DAG topology, RAIL operators, config/instances, doc structure) and the **QBO→Xero change list** to clone it. Start here. |
| [01-synch-firms.md](01-synch-firms.md) | Firm initial sync — full step-by-step Workato logic + Airflow design notes. |
| [02-synch-accounts.md](02-synch-accounts.md) | Chart-of-accounts sync (orchestrator + worker) — full logic + Airflow design notes. |
| [03-sync-tax-codes.md](03-sync-tax-codes.md) | Tax-code sync (delegated GL recipe, incl. component fan-out + compound linking) — full logic + Airflow design notes. |
| [04-lookup-tables.md](04-lookup-tables.md) | Lookup-table definitions for `vp_xero_integration/common/tables.py` (column maps, UNIQUE keys, static vs collection). |
| [05-open-questions.md](05-open-questions.md) | Consolidated open questions / decisions to resolve before/within the User Story stage. |
| [06-lookup-table-seeding.md](06-lookup-table-seeding.md) | `Mapping/Lookup Tables/` seeding recipes — how placeholder rows are created (answers Q-F3). |
| [07-validation.md](07-validation.md) | `Mapping/Validation/` recipes — referential-integrity checks + self-heal, consolidated validation rule checklist. |
| [08-xero-api-inventory.md](08-xero-api-inventory.md) | **Every Xero API call** across the whole `014-501 PSA` package, per-recipe, + RAIL `XeroAPIOperator` gap analysis (drives RAIL-operator story US-9). |

## Key cross-cutting facts (read before the detail docs)

1. **Direction is Xero → Vantagepoint** for all three initial-sync flows. Despite folder/recipe naming implying bidirectional, the *initial sync* reads from Xero and writes to VP, then records the cross-reference in the `014-501 PSA Map *` lookup tables.
2. **Workato "smart list / collection + `query_list` SQL"** is the heart of each recipe. The QBO Airflow port reproduces this with **run-local SQLite collections** (`rail.CreateCollectionOperator` + `rail.QueryCollectionOperator`) — load each source (Xero / VP / existing map) into a temp table, run the JOIN, iterate the result. The Xero port should do the same.
3. **Lookup tables become S3-backed SQLite collections** created up-front by one `rail.S3CreateMultiTableCollectionOperator` call (`init_mapping_collections`), with UNIQUE constraints enabling `INSERT OR REPLACE` idempotent upserts.
4. **Workato has no native upsert** — recipes delete-by-`EntryID`-then-add, or `update_entry` by `id`. In Airflow this maps to a UNIQUE key + `INSERT OR REPLACE`. The natural key per table is documented in [04-lookup-tables.md](04-lookup-tables.md).
5. **`=skip` vs `=blank`:** in the recipes, `=skip` means *omit the field from the payload* (leave VP value untouched); `=blank` means *send empty* (clear it). Preserve this distinction in the VP API calls.

## Source-of-truth paths

- Workato source: `integration_vantagepoint_xero/code/014-501 PSA/`
- Lookup-table schemas: `integration_vantagepoint_xero/code/014_501_psa_*.lookup_table.json`
- QBO Airflow reference: `airflow-integrations/dags/vp_quickbooks_integration/mapping_sync/` and `.../common/tables.py`
- RAIL operator library: `replicon-airflow-library/rail/rail/` (per `airflow-integrations/CLAUDE.md`: **use RAIL operators only; never create custom operators**).
