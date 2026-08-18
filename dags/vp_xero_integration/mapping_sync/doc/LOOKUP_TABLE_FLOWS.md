# Lookup-Table Flows — Workato source-of-truth + Airflow implementation guide

Flow + schema reference for the `mapping_sync` collections that are **not** the
three primary `map_*` mapping tables (those are documented by their per-table
fix logs). Covers the state machine, the seeded reference table, and the sibling
collections created up front by `init_mapping_collections`.

Source schemas: `integration_vantagepoint_xero/code/014_501_psa_*.lookup_table.json`.
Column constants: [`common/tables.py`](../../common/tables.py).

| Table | Category | Airflow status |
| --- | --- | --- |
| `mapping_table_state` | state machine | DONE — seeded + driven by dispatcher/children |
| `map_account_type` | seeded reference | DONE — seeded from Workato data at init |
| `map_employee` | sibling map (employee sync) | table created; populated by a future employee-sync DAG |
| `map_bank_code` | sibling map (bank resolution) | table created; populated out-of-band |
| `map_currency_code` | sibling map (currency sync) | table created; populated by a future currency-sync DAG |
| `outstanding_employee_expenses` | GL staging | table created; written by GL flows |
| `outstanding_purchase_invoices` | GL staging | table created; written by GL flows |

---

## 1. `mapping_table_state` — `014-501 PSA Mapping Table State`

The per-step state machine that sequences and gates the child DAGs.

### Workato source
`populate_mapping_state` seeds the step rows; `premapping` sets their initial
Status; each sync sub-recipe marks `Complete`; `validate_mapping_tables` marks
`Error` / final `Ready`.

### Schema
| col | Workato label | Airflow column | notes |
| --- | --- | --- | --- |
| col1 | Step | `Step` | canonical step name (`Map Firms` / `Map Accounts` / `Map Tax Codes`) |
| col2 | Recipe | `DagId` | renamed — carries the child DAG id, not a recipe name |
| col3 | Table | `TableName` | renamed — avoids the SQL reserved word `Table` |
| col4 | Status | `Status` | `'' \| 'Complete' \| 'Error' \| 'Ready'` |
| col5 | Messages | `Messages` | free-text diagnostic |
| col6 | Sequence | `Sequence` | `'10' / '20' / '30'` |

### Airflow implementation — DONE
- **Seed:** `seed_mapping_state_rows(instance)` (in `utils/_shared.py`) builds the
  3 rows from `MAPPING_STEPS_ORDERED`; passed as the `mapping_table_state` table
  `source` in `dispatcher_dag.init_mapping_collections`. Per-table preserve
  semantics mean the rows seed only on first create.
- **Premapping:** `apply_premapping_state()` reads `CFG_UpgradeDataSync` and sets
  every step `Status='Complete'` (false → trust external data, skip) or `''`
  (true → force re-sync). Content-aware override: if `false` but all map tables
  are empty, it flips to `''` so a fresh customer still syncs.
- **Per-step marks:** each `map_*_dag` calls `mark_step_status(step,'Complete')`
  on success; `validate_mappings` calls `mark_step_status(step,'Error')` on a
  hard-fail; the dispatcher bulk-sets `'Ready'` on the success path.
- **Skip gates:** each child DAG reads `check_step_status(step)` (primary) and
  `is_table_populated(table)` (secondary defensive) before populating.

> Employee is out of scope (Q1) so there is **no** `Map Employees` step row — the
> ordered steps are firm (10) / account (20) / tax (30).

---

## 2. `map_account_type` — `014-501 PSA Map Account Type`  (SEEDED, 16 rows)

Translation table: Xero account `Type` enum → VP numeric type code. Read by the
account sync at compile time.

### Workato source
Ships with `data` in the lookup-table export (16 rows). Seeded once; never
written by a recipe.

### Schema
| col | Workato label | Airflow column | notes |
| --- | --- | --- | --- |
| col1 | Description (Not Used) | — | dropped |
| col2 | Description | `Description` | human label |
| col3 | Type | `XeroType` | **join key** — Xero account-type enum (CURRENT, CURRLIAB, FIXED, …) |
| col4 | Vantagepoint Code | `VantagepointCode` | VP numeric type code written on the VP account |

### Airflow implementation — DONE
- Decision **Q7 = A**: kept as a **seeded S3 collection** (data-driven, matching
  Workato), not a static Python constant. The 16 seed rows are
  `ACCOUNT_TYPE_SEED_ROWS` in `common/tables.py`, ported verbatim from the
  Workato `data`.
- `dispatcher_dag.init_mapping_collections` creates the table with a UNIQUE index
  on `XeroType` and seeds it from `ACCOUNT_TYPE_SEED_ROWS`.
- `_account_sync._load_account_type_index` reads it into a dict
  (`XeroType.upper() → VantagepointCode`) at sync time. Xero account types with
  no row here are **surfaced in `Messages`, not dropped** (see
  [`MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md`](MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md) #1).

---

## 3. Sibling collections (created up front; not mapping_sync steps)

These tables are created empty by `init_mapping_collections` so the S3
collection has a complete schema, but they are **populated by other flows**, not
the firm/account/tax sync. Column constants are in `common/tables.py`; schemas
are sanitized from the authoritative `014_501_psa_*.lookup_table.json` sticky
labels.

| Table | Natural key (UNIQUE) | Populated by |
| --- | --- | --- |
| `map_employee` | `ContactID` | future employee-sync DAG (Xero Contact ↔ VP Employee) |
| `map_bank_code` | `XeroID` | bank-code resolution (VP bank acct ↔ Xero bank acct) |
| `map_currency_code` | `XeroCode` | future currency-sync DAG (sibling GL recipe) |
| `outstanding_employee_expenses` | — (transactional) | GL employee-expense export flow |
| `outstanding_purchase_invoices` | — (transactional) | GL AP-voucher export flow |

The `map_*` siblings carry a UNIQUE index for idempotent upserts; the
`outstanding_*` tables are transactional working state and intentionally carry
no key.

---

## Cross-cutting concerns

### Distributed locking
Reads that only need a snapshot (existing-map loads, the seeded type lookup, the
validators) open the collection with `use_lock=False` / `read_only=True`. Each
sync engine confines the S3 write lock to a **single** batched
`S3UpsertCollectionOperator` at the end (the 3-phase pattern: lock-free read →
VP API work → one locked write), so the lock is never held across VP HTTP
round-trips.

### UNIQUE constraints
Every `map_*` table declares its natural key as a UNIQUE index via
`init_mapping_collections` so re-runs **upsert** (`INSERT … ON CONFLICT`) rather
than stack duplicates: `map_firm`=ContactID, `map_chart_of_accounts`=XeroID,
`map_tax_code`=(XeroName, XeroCode), `map_account_type`=XeroType,
`map_employee`=ContactID, `map_bank_code`=XeroID, `map_currency_code`=XeroCode.

### State-row plumbing (`mapping_table_state`)
See §1. The Status column is the single coordination point between the
dispatcher, the child DAGs, and validation; the per-customer init Variable
(`vp_xero_mapping_init_{customerId}_{instance}`) is the outer one-shot gate.

## Summary table

| Table | Pop. in Workato | Pop. in Airflow | Where it belongs |
| --- | --- | --- | --- |
| `mapping_table_state` | populate_mapping_state | dispatcher seed + child marks | mapping_sync |
| `map_account_type` | lookup-table data | init seed (Q7=A) | mapping_sync |
| `map_employee` | synch_employees | (future employee-sync DAG) | sibling |
| `map_bank_code` | resolve_bank_code | (out-of-band) | sibling / GL |
| `map_currency_code` | sync_currency_codes | (future currency-sync DAG) | sibling / GL |
| `outstanding_employee_expenses` | GL expense flow | (GL flow) | GL |
| `outstanding_purchase_invoices` | GL voucher flow | (GL flow) | GL |
