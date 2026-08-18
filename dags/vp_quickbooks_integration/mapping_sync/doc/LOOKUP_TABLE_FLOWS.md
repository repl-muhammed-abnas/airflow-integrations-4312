# Lookup-Table Flows — Workato source-of-truth + Airflow implementation guide

This doc covers the **configuration** and **transaction-tracking** lookup
tables in the VP ↔ QuickBooks Workato package that are NOT part of the
four canonical mapping tables (`map_firm`, `map_employee`,
`map_account_code`, `map_tax_code` — those have their own
`MAP_*_SYNC_FIX_LOG.md` docs).

Scope:

| Table | Category | Airflow status |
|---|---|---|
| `bank_code_map` | Configuration | **Removed from mapping_sync** — bank-code resolution is transaction-time (see §1) |
| `invoice_section_code` | Static config (Python constant) | **Done — `INVOICE_SECTION_CODE_MAP`; not a collection** |
| `pay_terms` | Static config (Python constant) | **Done — `PAY_TERMS_MAP`; not a collection** |
| `outstanding_employee_expenses` | Transaction tracking | Planned |
| `outstanding_purchase_invoices` | Transaction tracking | Planned |
| `outstanding_sales_invoices` | Transaction tracking | Planned |

All six tables are created at dispatcher init time by
`S3CreateMultiTableCollectionOperator` in
`mapping_sync/dispatcher_dag.py:init_mapping_collections` — the schemas
are already in `mapping_sync/utils/tables.py`. What's missing for five
of them is the **population** logic.

> Workato recipe paths below are relative to
> `integration_vantagepoint_quickbooks/code/`.

---

## 1. `bank_code_map` — **NOT a mapping-sync step (removed)**

> **Superseded.** The eager mapping-init population of `bank_code_map`
> (`map_bank_code_dag.py` / `_bank_code_sync.py`) was **removed** after a
> Workato re-assessment: in Workato the Bank Code Map is populated *only*
> lazily by `resolve_bank_code` at transaction time (bill/invoice payment
> posting) — it is **not** part of the Mapping module. The Airflow eager
> S3 table also had **no reader**: the payment consumer
> (`invoice_payment_sync.resolve_bank_code_method`) resolves from the
> Airflow Variable `psa_vp_qbo_invoice_payment_bank_code_map_{instance}`,
> not this S3 collection. The empty `bank_code_map` table is still created
> at init (schema parity), but nothing populates it from mapping_sync.
> The Workato-source detail below is retained for historical reference.

### Workato source

- **Recipe**: `014-503 PSA/Common Functions/014_503_psa_resolve_bank_code.recipe.json`
- **Callers**:
  - `014-503 PSA/GL Functions/014_503_psa_quickbooks_bill_payment_adds_to_vantagepoint.recipe.json`
  - `014-503 PSA/GL Functions/014_503_psa_post_invoice_payment_to_vantagepoint.recipe.json`
- **NOT** in `populate_mapping_table.recipe.json` — Workato populates this
  table **lazily** on the payment hot path. First time a QBO bank-account
  ID is seen on a bill/invoice payment, the resolver recipe:
  1. `search_entries` by `col4` (QBO Account ID).
  2. If 0 entries → `GET account/{id}` (QBO), `add_entry` with QBO side
     only (col3=Name, col4=ID, col5=Active).
  3. Then `GET vision/BankCode/CFGBANKS` (VP), foreach VP code:
     `if QBO.Account.Name == VP.Description` → `update_entry` to fill
     the VP side (col1=Description, col2=Code, col6=Company, col7=Org,
     col8=Account).
  4. On miss → call error-logger, return error.

### Schema (Workato sticky columns → Airflow identifiers)

| col | label | Airflow column |
|---|---|---|
| col1 | Vantagepoint name | `VantagepointName` |
| col2 | Vantagepoint code | `VantagepointCode` |
| col3 | QBO name | `QBOName` |
| col4 | QBO ID | `QBOID` |
| col5 | Status | `Status` |
| col6 | Company | `Company` |
| col7 | Org | `Org` |
| col8 | Account | `Account` |

### Airflow port (MAP2-3312)

**Strategy**: eager populate at mapping-init time so payment-sync DAGs
find rows pre-built instead of doing the QBO + VP fetch + name match on
the hot path.

- **DAG**: `mapping_sync/map_bank_code_dag.py`
- **Step**: `MAPPING_STEP_BANK_CODE = 'Map Bank Codes'`, Sequence `'50'`
  in `mapping_table_state` (the 5th step row, Airflow-only — Workato has
  4 steps).
- **Position in dispatcher chain**: between `trigger_map_tax_code` and
  `trigger_transaction_tracking` (see `mapping_sync/dispatcher_dag.py`).
- **Sync helper**: `sync_qbo_bank_codes_to_vp(instance)` in
  `mapping_sync/utils/python_callable_method.py`.
- **Match policy**: strict byte-for-byte
  `QBO.Account.Name == VP.CFGBANKS.Description` (Workato parity).
- **No-match policy**: skip the QBO row entirely, log a warning. Only
  matched rows are written.
- **Write path**: ONE `S3UpdateCollectionOperator` invocation per
  dispatcher run, carrying a multi-row
  `INSERT OR REPLACE INTO bank_code_map (...) VALUES (?,?,...),(?,?,...),...`
  statement. The operator wraps `get_or_create_s3_collection_artifact`
  → distributed locking via S3 conditional PutObject (`IfMatch=<etag>`)
  is owned by the operator surface.
- **VP endpoint**: `vision/BankCode/CFGBANKS` via
  `rail.VantagepointSettingsBankOperator(request_method='GET')`.
- **QBO query**: `select * from Account where Active = true and AccountType = 'Bank'`
  via `rail.QuickBooksAccountOperator`.

### Open question

The Workato lazy resolver wrote the QBO side first and lazily filled the
VP side later. The Airflow eager port skips unmatched QBO banks
entirely. If a tenant has QBO bank accounts not named exactly the same
as their VP CFGBANKS rows, those payments won't resolve at run time
either — so a future Phase-5 validator could surface this as a warning
during `validate_mappings`. Not wired today.

---

## 2. `invoice_section_code` — static seed

### Workato source

- **Lookup-table file**: `014_503_psa_invoice_section_code.lookup_table.json`
- **Seed data** lives in the `.lookup_table.json` `"data"` field (8 rows,
  shipped with the package, NOT populated by any recipe):

  ```
  F,Fee
  L,Labor
  C,Consultant
  E,Expense
  U,Unit
  A,Add-on
  T,Tax
  I,Interest
  ```

- **Read-only consumers** (`get_entry` by `col1`):
  - `014-503 PSA/GL Functions/014_503_psa_post_invoice_to_quickbooks_us.recipe.json`
  - `014-503 PSA/GL Functions/014_503_psa_post_invoice_to_quickbooks_ca_uk.recipe.json`

  Used inside a `foreach` over VP invoice sections to translate the
  single-letter VP section code (F/L/C/E/U/A/T/I) → human-readable
  description on the outgoing QBO invoice line.

### Schema

| col | label | Airflow column |
|---|---|---|
| col1 | Code (sticky) | `Code` |
| col2 | Description (sticky) | `Description` |

### Airflow implementation (DONE)

This is **immutable static reference data**, so it is shipped as a Python
constant — `INVOICE_SECTION_CODE_MAP` in `utils/tables.py` — and is **NOT**
created as an S3 collection (there is no `invoice_section_code` table in
`init_mapping_collections`). Consumers import the constant directly. See
`doc/STATIC_CONFIG_LOOKUPS.md` for the rationale.

---

## 3. `pay_terms` — static seed

### Workato source

- **Lookup-table file**: `014_503_psa_pay_terms.lookup_table.json`
- **Seed data** (in `"data"` field, NOT populated by any recipe):

  ```
  0,Next
  1,Next
  2,15
  3,30
  4,60
  5,Next
  ```

  `col1` is the QBO numeric pay-term ID; `col2` is the VP pay-term
  string. Note `0`, `1`, `5` all map to `Next`.

- **Read-only consumer** (`search_entries` by `col1`):
  - `014-503 PSA/Common Functions/014_503_psa_dvp_insert_update_veaccounting.recipe.json`

  Called when creating/updating a VP Vendor Accounting Info record from
  QBO data. If the QBO vendor has a `PayTerms` value, the recipe looks
  it up here to find the matching VP pay-term string. Recipe inline
  comment (line ~378) notes: *"If pay terms passed in from QBO this
  will be a QBO ID so we will look this up from the mapping table. If
  no mapping found it will remain as the default value Next"*.

### Schema

| col | label | Airflow column |
|---|---|---|
| col1 | QB Pay Terms (sticky) | `QBPayTerms` |
| col2 | DVP Pay Terms (sticky) | `DVPPayTerms` |

### Airflow implementation (DONE)

Shipped as a Python constant — `PAY_TERMS_MAP` in `utils/tables.py` — and
**NOT** created as an S3 collection (there is no `pay_terms` table in
`init_mapping_collections`). QBO Term Ids were confirmed consistent across
tenants, so a single global mapping is valid. `vendor_sync.lookup_pay_terms`
reads `PAY_TERMS_MAP` directly (falling back to `_DEFAULT_PAY_TERMS = 'Next'`
for unmapped Ids). See `doc/STATIC_CONFIG_LOOKUPS.md`.

---

## 4. `outstanding_employee_expenses` — transaction tracking

### Workato source

A short-lived row tracks an employee expense report that has been
exported from VP to QBO but is **not yet paid**. The Workato flow is:

1. **Initial seed** (one-shot at deployment):
   `014-503 PSA/Deployment/014_503_psa_import_legacy_data.recipe.json`
   pulls in-flight expenses from VP and `add_entry`'s each.
2. **Per-export add**: when VP exports an expense to QBO as a Purchase:
   - `014-503 PSA/GL Functions/014_503_psa_post_employee_expense_to_quickbooks_us.recipe.json`
   - `014-503 PSA/GL Functions/014_503_psa_post_employee_expense_to_quickbooks_ca_uk.recipe.json`

   Each calls `add_entry` with `col1=Period, col2=PostSeq, col3=Employee,
   col6=Voucher, col7=TotalAmt, col8=QBO Purchase Id`.
3. **Partial-payment update** (rare for expense reports but covered):
   `014-503 PSA/GL Functions/014_503_psa_quickbooks_bill_payment_adds_to_vantagepoint.recipe.json`
   — when QBO sends a payment that doesn't zero out the balance, the
   matching row is `update_entry`'d (Outstanding Amount decremented).
4. **Full-payment delete**:
   `014-503 PSA/GL Functions/014_503_psa_vantagepoint_expense_report_exports_to_quickbooks_us.recipe.json`
   (and `..._ca_uk`) — `search_entries` by
   `Period+PostSeq+Employee+Voucher`, then `delete_entry` on match.
   `quickbooks_bill_payment_adds_to_vantagepoint` also deletes when
   Balance reaches 0.

### Schema

| col | Workato label | Airflow column |
|---|---|---|
| col1 | Period | `Period` |
| col2 | Post seq | `PostSeq` |
| col3 | Employee | `Employee` |
| col4 | Org | `Org` |
| col5 | Transaction date | `TransactionDate` |
| col6 | Voucher | `Voucher` |
| col7 | Outstanding amount | `OutstandingAmount` |
| col8 | Invoice ID (QBO entity Id) | `InvoiceID` |
| col9 | Messages | `Messages` |
| col10 | QBOID | `QBOID` |

### Airflow implementation guidance

This table's lifecycle does **not** belong inside `mapping_sync` — it's
driven by transactional flows that happen at runtime, not by one-shot
init. The mapping_sync dispatcher's `init_mapping_collections` already
creates the empty table; that's all `mapping_sync` should do for it.

The actual writes belong to future DAGs in a sibling package (call it
`gl_sync/` or `transaction_sync/` — pattern not yet established in this
repo). Each transactional DAG would:

- Read the matching row(s) via `S3QueryCollectionOperator`.
- Insert / update / delete via `S3UpdateCollectionOperator` (or
  `S3UpsertCollectionOperator` once the table gains a UNIQUE constraint
  on `(Period, PostSeq, Employee, Voucher)` — currently no constraint;
  see [Cross-cutting concerns](#cross-cutting-concerns) below).

**Initial seed (`import_legacy_data` parity)**: there's no Airflow
equivalent yet. Two options when we get to it:

- **A.** A one-shot DAG `transaction_sync/import_legacy_expenses_dag.py`
  triggered manually (or by a Variable gate similar to
  `vp_qbo_*_mapping_init`). Pulls open expenses from VP, populates the
  table, then never runs again.
- **B.** Skip legacy-import entirely — assume operators set up a fresh
  tenant where no in-flight expenses exist at cutover. This is what the
  current `transaction_tracking_dag.py` placeholder is presumably
  modeled on; needs business confirmation.

---

## 5. `outstanding_purchase_invoices` — transaction tracking

### Workato source

Mirrors `outstanding_employee_expenses` for AP vouchers:

1. **Initial seed**:
   `014-503 PSA/Deployment/014_503_psa_import_legacy_data.recipe.json`
   — `add_entry` per open AP voucher from VP.
2. **Per-export add**: when VP exports an AP voucher to QBO as a Bill:
   - `014-503 PSA/GL Functions/014_503_psa_post_ap_voucher_to_quickbooks_us.recipe.json`
   - `014-503 PSA/GL Functions/014_503_psa_post_ap_voucher_to_quickbooks_ca_uk.recipe.json`

   Each `search_entries` first by `col1=Batch, col2=Voucher` to detect
   duplicates, then `add_entry` if absent.
3. **Partial-payment update**:
   `014-503 PSA/GL Functions/014_503_psa_quickbooks_bill_payment_adds_to_vantagepoint.recipe.json`
   — `update_entry` setting `col7=Balance` when QBO bill payment leaves
   a positive balance.
4. **Full-payment delete**: same recipe — `delete_entry` when Balance
   reaches 0.

### Schema

| col | Workato label | Airflow column |
|---|---|---|
| col1 | Batch | `Batch` |
| col2 | Voucher | `Voucher` |
| col3 | WBS 1 | `WBS1` |
| col4 | WBS 2 | `WBS2` |
| col5 | WBS 3 | `WBS3` |
| col6 | Line amount | `LineAmount` |
| col7 | Outstanding amount | `OutstandingAmount` |
| col8 | Account | `Account` |
| col9 | Org | `Org` |
| col10 | Invoice ID (QBO Bill Id) | `InvoiceID` |

### Airflow implementation guidance

Same shape as `outstanding_employee_expenses`. Belongs in the future
`transaction_sync` package, NOT in `mapping_sync`. The table is created
empty by the multi-table init; lifecycle writes happen elsewhere.

Natural match key for upserts: `(Batch, Voucher)`. Worth adding a
UNIQUE constraint on those two columns when the table-create operator
gains support — would let consumers use
`S3UpsertCollectionOperator` instead of hand-built `INSERT OR REPLACE`
queries.

---

## 6. `outstanding_sales_invoices` — transaction tracking

### Workato source

Mirrors the AP path for AR invoices:

1. **Initial seed**:
   `014-503 PSA/Deployment/014_503_psa_import_legacy_data.recipe.json`
   — `add_entry` per open AR invoice from VP.
2. **Per-export add**: when VP exports an AR invoice to QBO:
   - `014-503 PSA/GL Functions/014_503_psa_post_invoice_to_quickbooks_us.recipe.json`
   - `014-503 PSA/GL Functions/014_503_psa_post_invoice_to_quickbooks_ca_uk.recipe.json`

   The export checks for an existing entry first via
   `014-503 PSA/Common Functions/014_503_psa_project_invoice_exists.recipe.json`
   (which `search_entries` by `col2=Invoice, col3=WBS1, col4=WBS2,
   col5=WBS3`), then `add_entry` if absent.
3. **Partial-payment update**:
   `014-503 PSA/GL Functions/014_503_psa_quickbooks_invoice_payment_adds_to_vantagepoint.recipe.json`
   — `update_entry` setting `col7=Balance`.
4. **Full-payment delete**: same recipe — `delete_entry` when Balance
   reaches 0.

### Schema

| col | Workato label | Airflow column |
|---|---|---|
| col1 | Batch | `Batch` |
| col2 | DVP Invoice | `DVPInvoice` |
| col3 | WBS1 | `WBS1` |
| col4 | WBS2 | `WBS2` |
| col5 | WBS3 | `WBS3` |
| col6 | Invoice Amount | `InvoiceAmount` |
| col7 | Outstanding Amount | `OutstandingAmount` |
| col8 | Transaction Date | `TransactionDate` |
| col9 | QBO Invoice | `QBOInvoice` |
| col10 | QBOID | `QBOID` |

### Airflow implementation guidance

Same shape as `outstanding_purchase_invoices`. Belongs in
`transaction_sync` (or wherever VP→QBO invoice export lands). Natural
match key: `(DVPInvoice, WBS1, WBS2, WBS3)` — matches the lookup the
Workato `project_invoice_exists` recipe does.

Note the slightly different lookup contract: `outstanding_sales_invoices`
has a dedicated existence-check recipe
(`014_503_psa_project_invoice_exists.recipe.json`) that downstream
flows reuse. The Airflow port should expose an equivalent helper
(`outstanding_invoice_exists(dvp_invoice, wbs1, wbs2, wbs3) -> dict | None`)
to keep that contract.

---

## Cross-cutting concerns

### Distributed locking

Every write to an S3-backed collection MUST go through an S3Collection
operator (`S3UpsertCollectionOperator`, `S3UpdateCollectionOperator`,
`S3QueryCollectionOperator`) — not directly through
`rail.lib.s3_collection.get_or_create_s3_collection_artifact` + a raw
sqlite cursor. The operator surface is where the ETag-based optimistic
concurrency control + conditional PutObject (`IfMatch=<etag>`) is
enforced; raw helper usage works but bypasses the canonical lock
surface. See `map_bank_code` for the multi-row-via-single-operator
pattern.

### UNIQUE constraints

`S3UpsertCollectionOperator` uses
`INSERT INTO … ON CONFLICT(key_columns) DO …`, which requires the
target table to have a `UNIQUE` / `PRIMARY KEY` index covering exactly
`key_columns`. Likewise, plain `INSERT OR REPLACE` only upserts when
such an index exists — without one it silently degrades to a plain
`INSERT`, so re-runs append duplicate rows instead of replacing.

`S3CreateMultiTableCollectionOperator` now accepts a per-table
`unique_columns` spec. When provided it creates a `UNIQUE` index over
exactly those columns, ensured idempotently on every run; a
pre-existing table that already accumulated duplicates under the old
constraint-less schema is **de-duplicated in place** (most-recent row
per key kept) before the index is added, so no table is dropped and no
data is lost beyond the duplicates. `map_firm` declares
`unique_columns=['QBOID', 'IsVendor']`
(`common.tables.MAP_FIRM_UNIQUE_COLUMNS`); its bulk write site
(`_firm_sync._upsert_map_firm_row`) keeps `INSERT OR REPLACE`, which now
upserts correctly against that key.

The other mapping tables (`map_employee`, `map_account_code`,
`map_tax_code`) and the outstanding-tracking tables have the same
latent duplicate-append risk and should each gain a `unique_columns`
spec keyed on their natural key once that key is confirmed (e.g.
`map_tax_code` → `(QBOCodeID, QBORateID)`). **Note** `account_type_map`
must NOT get a unique key on `QBOType` — its seed rows intentionally
repeat an empty `QBOType`.

### State-row plumbing (`mapping_table_state`)

Workato's `mapping_table_state` lookup tracks the four canonical
mapping steps. The Airflow port already extends it to 5 steps (added
`Map Bank Codes`, Sequence `'50'`). The transaction-tracking tables
above are **NOT** new steps in `mapping_table_state` — their lifecycle
sits outside the mapping-init one-shot model. Don't add them to
`MAPPING_STEPS_ORDERED`.

---

## Summary table

| Table | Pop. mechanism in Workato | Pop. mechanism in Airflow | Where it belongs |
|---|---|---|---|
| `bank_code_map` | Lazy on payment hot path | Lazy/manual at transaction time (`invoice_payment_sync`, Variable-based) | `invoice_payment_sync/` (NOT mapping_sync) |
| `invoice_section_code` | Static seed in `.lookup_table.json` | Python constant `INVOICE_SECTION_CODE_MAP` (not a collection) | `mapping_sync/utils/tables.py` |
| `pay_terms` | Static seed in `.lookup_table.json` | Python constant `PAY_TERMS_MAP` (not a collection) | `mapping_sync/utils/tables.py` |
| `outstanding_employee_expenses` | `import_legacy_data` + per-export add + payment update/delete | TBD — belongs in `transaction_sync/` (placeholder exists) | `transaction_sync/` (not mapping_sync) |
| `outstanding_purchase_invoices` | `import_legacy_data` + per-export add + payment update/delete | TBD — same as above | `transaction_sync/` |
| `outstanding_sales_invoices` | `import_legacy_data` + per-export add + payment update/delete | TBD — same as above | `transaction_sync/` |
