"""Shared table definitions for vp_quickbooks_integration collections.

Lives in the `common` package
(`vp_quickbooks_integration.common.tables`) so it can be imported across
the workflow folders (mapping_sync, bill_payment_sync, ...). Single source of
truth for collection table names + column lists, plus the static config lookups
(`PAY_TERMS_MAP` / `INVOICE_SECTION_CODE_MAP`).

Consumed by:
- `mapping_sync/dispatcher_dag.py:init_mapping_collections` (one
  S3CreateMultiTableCollectionOperator call that creates every collection table
  up-front)
- mapping_sync + bill_payment_sync helpers (SELECT / INSERT / UPDATE statements
  reference these for column ordering and SQLite identifier names)

Naming convention (mirrors Workato sticky-label columns):
- TABLE_NAME constants are snake_case literals matching the Workato
  lookup-table filename (e.g. `map_firm`, `bank_code_map`).
- COLUMNS constants are lists of PascalCase identifiers sanitized from
  each Workato sticky column's `label` (e.g. 'Is Vendor' → 'IsVendor',
  'Vantagepoint type [RO]' → 'VantagepointTypeRO', 'QBO ID' → 'QBOID').
  Non-sticky 'Untitled column N' slots from Workato are dropped — the
  SQLite schema is dense.

Source files in `integration_vantagepoint_quickbooks/code/*.lookup_table.json`.
"""

# ===========================================================================
# MAPPING TABLES (sticky columns only, Workato parity)
# ===========================================================================

MAP_FIRM_TABLE_NAME = 'map_firm'
# Workato map_firm has FirmID, QBOID, Is Vendor, Name. IsVendor stores
# 'Y' / 'N' (matches the Workato 'Is Vendor' label sanitized to a
# SQLite-friendly identifier).
MAP_FIRM_COLUMNS = ['FirmID', 'QBOID', 'IsVendor', 'Name']
# Natural key for the firm cross-reference. A QBO entity (QBOID) maps to
# exactly one VP firm per relationship side (IsVendor 'Y'/'N'), matching
# the in-memory index key in
# `mapping_sync.utils._firm_sync._load_existing_map_firm_index`. Declared
# as a UNIQUE index (via `init_mapping_collections`) so re-runs upsert on
# this key — `INSERT OR REPLACE` in `_upsert_map_firm_row` replaces the
# existing row instead of appending a duplicate. Before this constraint
# existed the table had no key, so `INSERT OR REPLACE` degraded to a
# plain INSERT and forced re-syncs stacked full duplicate copies. See
# mapping_sync/doc/LOOKUP_TABLE_FLOWS.md (UNIQUE constraints).
MAP_FIRM_UNIQUE_COLUMNS = ['QBOID', 'IsVendor']


MAP_EMPLOYEE_TABLE_NAME = 'map_employee'
# Workato map_employee has 5 named columns:
#   Employee      = VP Employee ID
#   QBOID         = QBO Employee Id
#   QBOVendorID   = QBO Vendor Id used for expense processing
#   QBOVendorName = QBO Vendor display name
#   Name          = display name
MAP_EMPLOYEE_COLUMNS = [
    'Employee',
    'QBOID',
    'QBOVendorID',
    'QBOVendorName',
    'Name',
]
# map_employee has TWO writers keyed differently, so the table carries TWO
# independent UNIQUE indexes (created via `init_mapping_collections` from
# MAP_EMPLOYEE_UNIQUE_INDEXES):
#   - QBOID    — employee_sync / mapping_sync write keyed by the QBO Employee Id
#   - Employee — employee_sync_upsert writes keyed by the VP Employee code
# Each is independently unique so each writer can do an atomic ON CONFLICT
# upsert on its own key (S3UpsertCollectionOperator keyed on the matching
# columns) instead of DELETE-then-INSERT, and the 1:1 VP-Employee <-> QBOID
# mapping is enforced. Without the matching index the ON CONFLICT upsert has no
# constraint and fails (rather than silently stacking duplicates) — same
# failure mode documented on MAP_FIRM_UNIQUE_COLUMNS.
#
# MAP_EMPLOYEE_UNIQUE_COLUMNS stays the FLAT ['QBOID'] list because it doubles
# as the ON CONFLICT key-column list for the QBOID-keyed upserts (employee_sync
# + mapping_sync `sync_qbo_employees_to_vp`); MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS
# is the flat key list for the Employee-keyed upsert (employee_sync_upsert). The
# table-create operator gets the combined MAP_EMPLOYEE_UNIQUE_INDEXES.
MAP_EMPLOYEE_UNIQUE_COLUMNS = ['QBOID']
MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS = ['Employee']
# Passed to S3CreateMultiTableCollectionOperator's per-table `unique_columns`
# in its list-of-lists form → one UNIQUE index per entry.
MAP_EMPLOYEE_UNIQUE_INDEXES = [
    MAP_EMPLOYEE_UNIQUE_COLUMNS,
    MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS,
]


MAP_ACCOUNT_CODE_TABLE_NAME = 'map_account_code'
# Workato map_account_code has 7 named columns. QBOID at the end mirrors
# the Workato 'QBO ID' label (the stable identifier used as the index
# key in _load_existing_map_account_index).
MAP_ACCOUNT_CODE_COLUMNS = [
    'QBOCode',
    'QBOName',
    'QBOType',
    'VantagepointCode',
    'VantagepointName',
    'VantagepointTypeRO',
    'QBOID',
]
# A QBO account can map to SEVERAL existing VP accounts matched by name
# (Workato's step-17 `va` fan-out — e.g. Advertising → VP 400 AND 6000), so
# the key pairs QBOID with VantagepointCode rather than QBOID alone. The
# UNIQUE index over both makes the sync's batched upsert idempotent (one row
# per distinct VP code per QBO account; QBO-only rows collapse to a single
# (QBOID, '') row) while letting the fan-out rows coexist. Consumed by
# `sync_qbo_accounts_to_vp`'s S3UpsertCollectionOperator key_columns and
# declared on the table via `dispatcher_dag.init_mapping_collections`. Mirrors
# MAP_TAX_CODE_UNIQUE_COLUMNS / MAP_FIRM_UNIQUE_COLUMNS.
MAP_ACCOUNT_CODE_UNIQUE_COLUMNS = ['QBOID', 'VantagepointCode']


MAP_TAX_CODE_TABLE_NAME = 'map_tax_code'
# Workato map_tax_code has 9 named columns. The Workato schema has a
# gap at col5 (no sticky label); we omit it so the resulting SQLite
# schema is dense rather than carrying a NULL placeholder.
# Match key for upsert: (QBOCodeID, QBORateID).
MAP_TAX_CODE_COLUMNS = [
    'QBOCodeName',
    'QBORateName',
    'QBOCodeID',
    'VantagepointCode',
    'Rate',
    'TaxTypeApplicable',
    'QBORateID',
    'IsTaxGroup',
    'TaxOn',
]
# A QBO rate can map to SEVERAL existing VP tax codes matched by name
# (Workato's step-17 `vtc` fan-out — e.g. NO TAX PURCHASE → VP 6,7,8,9), so
# the key includes VantagepointCode. The UNIQUE index over all three makes
# the sync's INSERT OR REPLACE idempotent (one row per distinct VP code per
# QBO rate component) while still allowing the fan-out rows to coexist.
MAP_TAX_CODE_UNIQUE_COLUMNS = ['QBOCodeID', 'QBORateID', 'VantagepointCode']


# ===========================================================================
# OUTSTANDING / STATE TABLES
# ===========================================================================

OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME = 'outstanding_employee_expenses'
# Pending employee expense reports awaiting sync.
OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS = [
    'Period',
    'PostSeq',
    'Employee',
    'Org',
    'TransactionDate',
    'Voucher',
    'OutstandingAmount',
    'InvoiceID',
    'Messages',
    'QBOID',
]


OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME = 'outstanding_purchase_invoices'
# Pending AP vouchers awaiting sync.
OUTSTANDING_PURCHASE_INVOICES_COLUMNS = [
    'Batch',
    'Voucher',
    'WBS1',
    'WBS2',
    'WBS3',
    'LineAmount',
    'OutstandingAmount',
    'Account',
    'Org',
    'InvoiceID',
]


OUTSTANDING_SALES_INVOICES_TABLE_NAME = 'outstanding_sales_invoices'
# Pending AR invoices awaiting sync.
OUTSTANDING_SALES_INVOICES_COLUMNS = [
    'Batch',
    'DVPInvoice',
    'WBS1',
    'WBS2',
    'WBS3',
    'InvoiceAmount',
    'OutstandingAmount',
    'TransactionDate',
    'QBOInvoice',
    'QBOID',
]

MAPPING_TABLE_STATE_TABLE_NAME = 'mapping_table_state'
# Per-step state for the 4 mapping syncs (firm, employee, account_code,
# tax_code — Workato parity). Mirrors Workato lookup
# `014_503_psa_mapping_table_state` (sticky columns col1-col6) with two
# Airflow-friendly column renames:
#   - `TableName` (col3) avoids the SQL reserved word `Table`.
#   - `DagId` (col2) replaces Workato's `Recipe` since on the Airflow
#     side this carries the child DAG id, not a recipe name.
#
# Lifecycle (see `apply_premapping_state` + per-DAG mark tasks):
#   Step                ← canonical step name (matches Workato seed)
#   DagId               ← Airflow child DAG id (Workato col2: `Recipe`)
#   TableName           ← canonical S3 collection table for the step
#   Status              ← '' | 'Complete' | 'Error' | 'Ready'
#   Messages            ← free-text error/diagnostic detail
#   Sequence            ← '10' / '20' / '30' / '40' (matches Workato)
MAPPING_TABLE_STATE_COLUMNS = [
    'Step',
    'DagId',
    'TableName',
    'Status',
    'Messages',
    'Sequence',
]


# ===========================================================================
# STATIC CONFIG LOOKUPS (Python constants — NOT S3 collections)
# ===========================================================================
# pay_terms and invoice_section_code are static, read-only config ported from
# the Workato package `data` field — no recipe writes them. They are shipped as
# plain Python constants instead of S3 collections and are intentionally NOT
# created by `init_mapping_collections`. See mapping_sync/doc/STATIC_CONFIG_LOOKUPS.md.

# QBO Term reference Id (string) -> VP pay terms. QBO Term Ids are consistent
# across our tenants (confirmed), so a single global mapping is valid.
# Consumed by vendor_sync.lookup_pay_terms.
PAY_TERMS_MAP = {
    '0': 'Next',
    '1': 'Next',
    '2': '15',
    '3': '30',
    '4': '60',
    '5': 'Next',
}

# Standard Deltek Vantagepoint invoice section codes (VP product constants,
# universal across tenants): VP section code -> description.
INVOICE_SECTION_CODE_MAP = {
    'F': 'Fee',
    'L': 'Labor',
    'C': 'Consultant',
    'E': 'Expense',
    'U': 'Unit',
    'A': 'Add-on',
    'T': 'Tax',
    'I': 'Interest',
}

# QBO AccountType -> {VP numeric type code, VP type display name}.
# Static, read-only product mapping ported verbatim from the Workato
# `014_503_psa_account_type_map.lookup_table.json` data. QBO's AccountType
# enum and VP's numeric type codes are product-level constants (not
# tenant-specific), so a single global mapping is valid. Only the
# QBO-matchable rows are kept — the Workato seed also carried 6 rows with
# an empty QBO Type (VP-only types: Net Worth=3, Reimbursable Consultant=6,
# Direct=7, Direct Consultant=8, Indirect=9, Other Charges=10) which never
# match a QBO AccountType during sync. QBO accounts whose type is not in
# this map are stored with an empty VP mapping (Workato parity).
# Consumed by mapping_sync._account_sync.sync_qbo_accounts_to_vp.
ACCOUNT_TYPE_MAP = {
    'Asset':     {'code': '1', 'name': 'Asset'},
    'Equity':    {'code': '2', 'name': 'Liability'},
    'Expense':   {'code': '5', 'name': 'Reimbursable Expense'},
    'Liability': {'code': '2', 'name': 'Liability'},
    'Revenue':   {'code': '4', 'name': 'Revenue'},
}


# ===========================================================================
# CONFIGURATION TABLES (Workato lookup_table.json equivalents — S3 collections)
# ===========================================================================

BANK_CODE_MAP_TABLE_NAME = 'bank_code_map'
# Workato `014_503_psa_bank_code_map.lookup_table.json` — 8 sticky columns.
BANK_CODE_MAP_COLUMNS = [
    'VantagepointName',
    'VantagepointCode',
    'QBOName',
    'QBOID',
    'Status',
    'Company',
    'Org',
    'Account',
]


# ===========================================================================
# MAPPING STEPS — Workato `populate_mapping_state` seed parity
# ===========================================================================
# The four canonical mapping steps. Step names match the Workato recipe
# `014_503_psa_populate_mapping_state.recipe.json` (recipe lines
# 139-164) verbatim — child DAGs and helpers reference these constants
# rather than re-hardcoding the strings.

MAPPING_STEP_FIRM = 'Map Firms'
MAPPING_STEP_EMPLOYEE = 'Map Employees'
MAPPING_STEP_ACCOUNT = 'Map Accounts'
MAPPING_STEP_TAX_CODE = 'Map Tax Codes'

# Ordered tuples used by `seed_mapping_state_rows` to populate the
# initial step rows in `mapping_table_state`. Each entry is
# (Step, TableName, Sequence) — `DagId` is filled at runtime from the
# per-instance Airflow dag_id.
MAPPING_STEPS_ORDERED = [
    (MAPPING_STEP_FIRM,      MAP_FIRM_TABLE_NAME,         '10'),
    (MAPPING_STEP_EMPLOYEE,  MAP_EMPLOYEE_TABLE_NAME,     '20'),
    (MAPPING_STEP_ACCOUNT,   MAP_ACCOUNT_CODE_TABLE_NAME, '30'),
    (MAPPING_STEP_TAX_CODE,  MAP_TAX_CODE_TABLE_NAME,     '40'),
]
