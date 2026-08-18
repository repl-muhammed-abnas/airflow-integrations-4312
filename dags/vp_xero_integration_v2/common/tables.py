"""Shared table definitions for vp_xero_integration collections.

Lives in the `common` package (`vp_xero_integration_v2.common.tables`) so it can
be imported across the workflow folders (mapping_sync, ...). Single source of
truth for collection table names + column lists + UNIQUE keys, plus the seeded
reference data (`ACCOUNT_TYPE_SEED_ROWS`).

Mirrors the QuickBooks `vp_quickbooks_integration/common/tables.py`, re-keyed
for Xero per aidlc-docs reverse-engineering doc 04-lookup-tables.md. Employee
mapping is out of scope (Q1 = No employee sync), so there is no `map_employee`.

Naming convention (mirrors Workato sticky-label columns):
- TABLE_NAME constants are snake_case literals matching the Workato
  lookup-table filename (e.g. `map_firm`, `map_chart_of_accounts`).
- COLUMNS constants are lists of PascalCase identifiers sanitized from each
  Workato sticky column's `label` (e.g. 'Xero Code' -> 'XeroCode'). Non-sticky
  'Untitled column N' / 'Description (Not Used)' slots are dropped — the SQLite
  schema is dense.

Source files: `integration_vantagepoint_xero/code/014_501_psa_*.lookup_table.json`.
"""

# ===========================================================================
# MAPPING TABLES (sticky columns only, Workato parity)
# ===========================================================================

MAP_FIRM_TABLE_NAME = 'map_firm'
# Workato `014_501_psa_map_firm` sticky columns (col1-col8). Xero firms have no
# VP-stored id, so Workato matches by Name at sync time; the table's stable
# cross-reference key is the Xero ContactID. Vendor/Client are the codes parsed
# from the Xero AccountNumber (`PL…` -> Vendor, `SL…` -> Client).
MAP_FIRM_COLUMNS = [
    'FirmID',
    'ContactID',
    'Status',
    'Vendor',
    'Client',
    'XeroName',
    'VantagepointName',
    'ModDate',
]
# A Xero contact (ContactID) maps to exactly one VP firm. Declared as a UNIQUE
# index (via `init_mapping_collections`) so re-runs upsert on this key —
# `INSERT OR REPLACE` replaces the existing row instead of appending a
# duplicate. Mirrors MAP_*_UNIQUE_COLUMNS in the QBO package.
MAP_FIRM_UNIQUE_COLUMNS = ['ContactID']


MAP_CHART_OF_ACCOUNTS_TABLE_NAME = 'map_chart_of_accounts'
# Workato `014_501_psa_map_chart_of_accounts` sticky columns (col1-col8).
# `XeroID` (the Xero AccountID) is the stable identifier used as the upsert key.
MAP_CHART_OF_ACCOUNTS_COLUMNS = [
    'XeroCode',
    'XeroName',
    'XeroType',
    'VantagepointCode',
    'VantagepointName',
    'VantagepointType',
    'XeroID',
    'Messages',
]
# A Xero account (XeroID) maps to one VP code; matching also falls back to
# VantagepointCode / XeroCode. Use XeroID as the upsert key so re-runs are
# idempotent (rows with a blank XeroID are skipped by the engine).
MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS = ['XeroID']


MAP_ACCOUNT_TYPE_TABLE_NAME = 'map_account_type'
# Translation table: Xero account `Type` enum -> VP numeric type code. Unlike
# the other map tables this is SEEDED reference data (Q7 = A: data-driven
# seeded S3 collection, not a static Python constant) — `init_mapping_collections`
# populates it from ACCOUNT_TYPE_SEED_ROWS below. Workato col1
# 'Description (Not Used)' is dropped; the join key is the Xero Type enum.
MAP_ACCOUNT_TYPE_COLUMNS = ['Description', 'XeroType', 'VantagepointCode']
MAP_ACCOUNT_TYPE_UNIQUE_COLUMNS = ['XeroType']
# Seed rows ported verbatim from the Workato
# `014_501_psa_map_account_type.lookup_table.json` `data` field (16 rows).
# Each entry is (Description, XeroType, VantagepointCode) — column order matches
# MAP_ACCOUNT_TYPE_COLUMNS. Xero's account-type enum and VP's numeric type codes
# are product-level constants (not tenant-specific), so a single global seed is
# valid. NON-CURRENT LIABILITY maps to VP code '2' (Workato 'data' carried a
# trailing-empty cell for that row; the analysis confirms '2').
ACCOUNT_TYPE_SEED_ROWS = [
    ('Current Asset',         'CURRENT',     '1'),
    ('Overhead',              'OVERHEADS',   '9'),
    ('Current Liability',     'CURRLIAB',    '2'),
    ('Liability',             'LIABILITY',   '2'),
    ('Non-current Liability', 'TERMLIAB',    '2'),
    ('Other Income',          'OTHERINCOME', '10'),
    ('Revenue',               'REVENUE',     '4'),
    ('Sales',                 'SALES',       '4'),
    ('Fixed Asset',           'FIXED',       '1'),
    ('Inventory',             'INVENTORY',   '1'),
    ('Non-current Asset',     'NONCURRENT',  '1'),
    ('Prepayment',            'PREPAYMENT',  '1'),
    ('Equity',                'EQUITY',      '3'),
    ('Depreciation',          'DEPRECIATN',  '9'),
    ('Direct Costs',          'DIRECTCOSTS', '7'),
    ('Expense',               'EXPENSE',     '9'),
]


MAP_TAX_CODE_TABLE_NAME = 'map_tax_code'
# Workato `014_501_psa_map_tax_code` sticky columns (col1-col7). A Xero TaxRate
# fans out to one row per nested TaxComponent, so the natural key is
# (XeroName, XeroCode) = (RateName, ComponentName). `VantagepointCode` is the
# generated `X####` code; `CompoundOnCode` links a compound component to its
# base component's VP code; `Sequence` is the high-water-mark counter.
MAP_TAX_CODE_COLUMNS = [
    'XeroName',
    'XeroCode',
    'VantagepointCode',
    'Rate',
    'CompoundOnCode',
    'Sequence',
    'Messages',
]
# One VP tax code per (RateName, ComponentName) — component fan-out. The UNIQUE
# index makes the sync's `INSERT OR REPLACE` idempotent across re-runs while
# letting the fan-out rows coexist.
MAP_TAX_CODE_UNIQUE_COLUMNS = ['XeroName', 'XeroCode']


# ===========================================================================
# ADDITIONAL COLLECTIONS (created up front by init_mapping_collections, but NOT
# mapping_sync firm/account/tax steps — consumed by sibling integrations:
# employee sync, bank-code resolution, currency sync, and the outstanding GL
# staging tables). Column lists are sanitized from the authoritative Workato
# `014_501_psa_*.lookup_table.json` sticky labels.
# ===========================================================================

MAP_EMPLOYEE_TABLE_NAME = 'map_employee'
# Workato `014_501_psa_map_employee` sticky columns (col1-col7). Xero employees
# are Contacts, so the cross-reference key is the Xero ContactID.
MAP_EMPLOYEE_COLUMNS = [
    'Employee',
    'ContactID',
    'Status',
    'AccountNumber',
    'CreatedDate',
    'ModDate',
    'Messages',
]
MAP_EMPLOYEE_UNIQUE_COLUMNS = ['ContactID']


MAP_BANK_CODE_TABLE_NAME = 'map_bank_code'
# Workato `014_501_psa_map_bank_code` sticky columns (col1-col8). Maps a VP bank
# account to its Xero bank account (XeroID); carries the Company/Org/Account
# posting context.
MAP_BANK_CODE_COLUMNS = [
    'VantagepointName',
    'VantagepointCode',
    'XeroName',
    'XeroID',
    'Status',
    'Company',
    'Org',
    'Account',
]
MAP_BANK_CODE_UNIQUE_COLUMNS = ['XeroID']


MAP_CURRENCY_CODE_TABLE_NAME = 'map_currency_code'
# Workato `014_501_psa_map_currency_code` sticky columns (col1-col4).
MAP_CURRENCY_CODE_COLUMNS = ['XeroName', 'XeroCode', 'VantagepointCode', 'Messages']
MAP_CURRENCY_CODE_UNIQUE_COLUMNS = ['XeroCode']


OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME = 'outstanding_employee_expenses'
# Workato `014_501_psa_outstanding_employee_expenses` sticky columns (col1-col9).
# Transactional GL staging — no natural key (rows are working state, not a map).
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
]


OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME = 'outstanding_purchase_invoices'
# Workato `014_501_psa_outstanding_purchase_invoices` sticky columns (col1-col10).
# Transactional GL staging — no natural key.
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


# ===========================================================================
# STATE / TRACKING TABLES
# ===========================================================================

MAPPING_TABLE_STATE_TABLE_NAME = 'mapping_table_state'
# Per-step state for the 3 mapping syncs (firm, account, tax — Workato parity).
# Mirrors Workato lookup `014_501_psa_mapping_table_state` (sticky columns
# col1-col6) with two Airflow-friendly renames:
#   - `TableName` (col3) avoids the SQL reserved word `Table`.
#   - `DagId` (col2) replaces Workato's `Recipe` since on the Airflow side this
#     carries the child DAG id, not a recipe name.
#
# Lifecycle (see `apply_premapping_state` + per-DAG mark tasks):
#   Step      <- canonical step name (matches Workato seed)
#   DagId     <- Airflow child DAG id (Workato col2: `Recipe`)
#   TableName <- canonical S3 collection table for the step
#   Status    <- '' | 'Complete' | 'Error' | 'Ready'
#   Messages  <- free-text error/diagnostic detail
#   Sequence  <- '10' / '20' / '30' (matches Workato)
MAPPING_TABLE_STATE_COLUMNS = [
    'Step',
    'DagId',
    'TableName',
    'Status',
    'Messages',
    'Sequence',
]


# ===========================================================================
# MAPPING STEPS — Workato `populate_mapping_state` seed parity
# ===========================================================================
# The three canonical mapping steps (employee descoped per Q1). Step names
# match the Workato recipe verbatim — child DAGs and helpers reference these
# constants rather than re-hardcoding the strings.

MAPPING_STEP_FIRM = 'Map Firms'
MAPPING_STEP_ACCOUNT = 'Map Accounts'
MAPPING_STEP_TAX_CODE = 'Map Tax Codes'

# Ordered tuples used by `seed_mapping_state_rows` to populate the initial step
# rows in `mapping_table_state`. Each entry is (Step, TableName, Sequence) —
# `DagId` is filled at runtime from the per-instance Airflow dag_id. Firm is
# sequenced first (root entity); tax last.
MAPPING_STEPS_ORDERED = [
    (MAPPING_STEP_FIRM,     MAP_FIRM_TABLE_NAME,               '10'),
    (MAPPING_STEP_ACCOUNT,  MAP_CHART_OF_ACCOUNTS_TABLE_NAME,  '20'),
    (MAPPING_STEP_TAX_CODE, MAP_TAX_CODE_TABLE_NAME,           '30'),
]
