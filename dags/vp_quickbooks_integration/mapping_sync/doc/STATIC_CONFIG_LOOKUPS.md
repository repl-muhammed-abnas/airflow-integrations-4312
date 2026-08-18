# Static config lookups: `pay_terms` & `invoice_section_code`

**Status:** Analysis / decision record · **Date:** 2026-06-08
**Audience:** Developers building / maintaining `vp_quickbooks_integration` Airflow DAGs

## TL;DR

The Workato lookup tables **`014-503 PSA Pay Terms`** and
**`014-503 PSA Invoice Section Code`** are **static, read-only configuration**.
No recipe ever writes to them; their rows ship as developer-maintained seed data
in the package. They can therefore be shipped in Airflow as **plain Python config
constants** — there is no need to model them as S3 collections.

This is in deliberate contrast to `bank_code_map`, `outstanding_purchase_invoices`,
`outstanding_employee_expenses`, `outstanding_sales_invoices`, and the `map_*`
tables, which **are** written at runtime and must remain S3 collections (see
[`LOOKUP_TABLE_FLOWS.md`](./LOOKUP_TABLE_FLOWS.md)).

---

## 1. The data

### `pay_terms` — `014_503_psa_pay_terms.lookup_table.json`
Sticky columns: `col1 = QB Pay Terms`, `col2 = DVP Pay Terms`.

| QB Pay Terms (QBO Term Id) | DVP Pay Terms |
|---|---|
| 0 | Next |
| 1 | Next |
| 2 | 15 |
| 3 | 30 |
| 4 | 60 |
| 5 | Next |

### `invoice_section_code` — `014_503_psa_invoice_section_code.lookup_table.json`
Sticky columns: `col1 = Code`, `col2 = Description`.

| Code | Description |
|---|---|
| F | Fee |
| L | Labor |
| C | Consultant |
| E | Expense |
| U | Unit |
| A | Add-on |
| T | Tax |
| I | Interest |

These are the standard Deltek Vantagepoint invoice **section codes** — VP product
constants, identical for every tenant.

---

## 2. Are they static? (evidence)

Yes. A full sweep of `integration_vantagepoint_quickbooks/code/014-503 PSA/` for
lookup-table operations against these two tables shows **only reads**:

| Table | Recipe | Action | Read/Write |
|---|---|---|---|
| Pay Terms | `Common Functions/014_503_psa_dvp_insert_update_veaccounting` | `search_entries` | read |
| Pay Terms | `GL Functions/014_503_psa_post_invoice_to_quickbooks_{us,ca_uk}` | `search_entries` / `get_entry` | read |
| Invoice Section Code | `GL Functions/014_503_psa_post_invoice_to_quickbooks_{us,ca_uk}` | `get_entry` | read |

There is **no** `add_entry` / `update_entry` / `delete_entry` against either table
anywhere in the recipe set. (The `add_entry`/`update_entry` calls that *do* appear
in the `post_invoice_to_quickbooks_*` recipes target a different table —
`Outstanding Sales Invoices` — not these two.)

## 3. How is the data populated?

- The rows live in the `data` field of each `.lookup_table.json` and are deployed
  with the Workato package. They are **developer-maintained seed data**, not
  generated at runtime.
- In Workato, these tables are edited manually (UI / package) and read by recipes;
  nothing in the integration mutates them during normal operation.
- **In Airflow the data lives as Python constants, not collections.**
  `common/tables.py` ships `PAY_TERMS_MAP` and
  `INVOICE_SECTION_CODE_MAP`. The `pay_terms` / `invoice_section_code` table
  definitions and their `init_mapping_collections` specs were **removed** (they
  were created empty and never populated), since static read-only data has no
  reason to round-trip through S3. Consumers import the constants directly.
  `account_type_map` was later converted the same way — it now ships as the
  `ACCOUNT_TYPE_MAP` constant in `common/tables.py` and is no longer a seeded
  collection (see `MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md` #5).

## 4. Decision: ship as global config constants

Because the data is static, read-only, and small, it is modelled as Python
constants rather than S3 collections. This avoids an S3 round-trip for fixed data
and avoids creating empty collections that nothing populates. The
`pay_terms` / `invoice_section_code` table definitions and their
`init_mapping_collections` specs have been removed accordingly.

```python
# Static lookups ported from the Workato package. Read-only — no recipe writes
# them. QBO Term Ids are consistent across our tenants (confirmed), so a single
# global pay-terms mapping is valid.
PAY_TERMS_MAP = {            # QBO Term Id -> Vantagepoint pay terms
    '0': 'Next', '1': 'Next', '2': '15', '3': '30', '4': '60', '5': 'Next',
}
INVOICE_SECTION_CODE_MAP = {  # VP invoice section code -> description
    'F': 'Fee', 'L': 'Labor', 'C': 'Consultant', 'E': 'Expense',
    'U': 'Unit', 'A': 'Add-on', 'T': 'Tax', 'I': 'Interest',
}
```

Notes:
- Keys are **strings** — QBO `SalesTermRef.value` and VP section codes both arrive
  as strings.
- **`pay_terms` caveat (resolved):** `col1` values are QBO **Term reference Ids**,
  which QBO assigns per company. A single global mapping is only correct if every
  tenant's QBO uses the same Term Ids. This has been **confirmed consistent across
  our tenants**, so the global mapping is valid. If a future tenant is provisioned
  with different QBO Term Ids, revisit this (move `pay_terms` to a per-tenant
  Variable or seed it per-customer into the `pay_terms` collection).
- **`invoice_section_code`** is universal (standard VP codes), so it is globally
  safe with no caveat.

**Suggested home:** `common/tables.py` — already the shared
single-source-of-truth for these tables' names/columns and imported cross-package
(e.g. by `bill_payment_sync`). Keeping the static data next to the schema constants
keeps them discoverable and reusable. A dedicated `shared/static_lookups.py` module
is an acceptable alternative.

## 5. What stays a collection (for contrast)

| Table | Static? | Airflow representation |
|---|---|---|
| `pay_terms` | ✅ read-only config | **Python constant** |
| `invoice_section_code` | ✅ read-only config | **Python constant** |
| `bank_code_map` | ❌ written at runtime (insert-on-miss) | S3 collection |
| `outstanding_purchase_invoices` | ❌ decremented / deleted on payment | S3 collection |
| `outstanding_employee_expenses` | ❌ deleted on payment | S3 collection |
| `outstanding_sales_invoices` | ❌ added / updated / deleted on export | S3 collection |
| `map_firm` / `map_employee` / `map_account_code` / `map_tax_code` | ❌ populated by mapping_sync | S3 collection |
| `account_type_map` | ✅ read-only config | **Python constant** (`ACCOUNT_TYPE_MAP` in `common/tables.py`) — no longer a seeded collection |

## 6. References
- Source lookup files: `integration_vantagepoint_quickbooks/code/014_503_psa_pay_terms.lookup_table.json`, `…/014_503_psa_invoice_section_code.lookup_table.json`
- Consuming recipes: `…/014-503 PSA/Common Functions/014_503_psa_dvp_insert_update_veaccounting.recipe.json`, `…/014-503 PSA/GL Functions/014_503_psa_post_invoice_to_quickbooks_us.recipe.json`, `…_ca_uk.recipe.json`
- Related: [`LOOKUP_TABLE_FLOWS.md`](./LOOKUP_TABLE_FLOWS.md) (per-table lifecycle for the dynamic collections), `common/tables.py` (table name/column constants)
