# 04 — Lookup-Table Definitions for `vp_xero_integration/common/tables.py`

Defines the Xero (`014-501`) lookup tables the three Initial-Sync DAGs read/write, in the
same form as the QBO `common/tables.py` (TABLE_NAME constant + COLUMNS list + UNIQUE columns).

**Conventions (from QBO `tables.py`):**
- `*_COLUMNS` = PascalCase identifiers sanitized from each Workato **sticky** column `label` (e.g. `Xero Code` → `XeroCode`, `Vantagepoint Code` → `VantagepointCode`).
- Non-sticky / "Untitled column N" slots are dropped (dense SQLite schema).
- Tables get a UNIQUE index on the natural key so `INSERT OR REPLACE` upserts are idempotent.
- Source schemas: `integration_vantagepoint_xero/code/014_501_psa_*.lookup_table.json`.

> **Note on QBO vs Xero column drift:** the Xero `map_*` tables do **not** match the QBO column lists. QBO `map_firm` is `[FirmID, QBOID, IsVendor, Name]`; Xero `map_firm` has 8 columns. Define Xero's own constants — do not reuse QBO's.

---

## `map_firm` — `014-501 PSA Map Firm`
Natural/match key: **ContactID** (Xero). (Workato matches firms by **Name** at sync time, but the table's stable cross-ref key is ContactID.)

| Workato col | label | Airflow column | notes |
| --- | --- | --- | --- |
| col1 | Firm ID | `FirmID` | VP ClientID |
| col2 | Contact ID | `ContactID` | Xero ContactID — **UNIQUE key** |
| col3 | Status | `Status` | Xero ContactStatus |
| col4 | Vendor | `Vendor` | code parsed from Xero AccountNumber (`PL…`) |
| col5 | Client | `Client` | code parsed from Xero AccountNumber (`SL…`) |
| col6 | Xero Name | `XeroName` | |
| col7 | Vantagepoint Name | `VantagepointName` | |
| col8 | Mod Date | `ModDate` | |

```python
MAP_FIRM_TABLE_NAME = 'map_firm'
MAP_FIRM_COLUMNS = ['FirmID', 'ContactID', 'Status', 'Vendor', 'Client',
                    'XeroName', 'VantagepointName', 'ModDate']
MAP_FIRM_UNIQUE_COLUMNS = ['ContactID']
```

---

## `map_chart_of_accounts` — `014-501 PSA Map Chart of Accounts`
Natural/match key: **XeroID** (with fallback match on VantagepointCode / Xero Code). Upsert by row id in Workato.

| Workato col | label | Airflow column |
| --- | --- | --- |
| col1 | Xero Code | `XeroCode` |
| col2 | Xero Name | `XeroName` |
| col3 | Xero Type | `XeroType` |
| col4 | Vantagepoint Code | `VantagepointCode` |
| col5 | Vantagepoint Name | `VantagepointName` |
| col6 | Vantagepoint Type | `VantagepointType` |
| col7 | Xero ID | `XeroID` |
| col8 | Messages | `Messages` |

```python
MAP_CHART_OF_ACCOUNTS_TABLE_NAME = 'map_chart_of_accounts'
MAP_CHART_OF_ACCOUNTS_COLUMNS = ['XeroCode', 'XeroName', 'XeroType',
                                 'VantagepointCode', 'VantagepointName',
                                 'VantagepointType', 'XeroID', 'Messages']
# A Xero account (XeroID) maps to one VP code; matching also falls back to
# VantagepointCode / XeroCode. Use XeroID as the upsert key (skip blank-XeroID rows).
MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS = ['XeroID']
```

---

## `map_account_type` — `014-501 PSA Map Account Type`  (SEEDED reference data, ~16 rows)
Translation table: Xero account `Type` → VP numeric type code. **Has data in the Workato export.**

| Workato col | label | Airflow column | notes |
| --- | --- | --- | --- |
| col1 | Description (Not Used) | `DescriptionNotUsed` | informational; can drop |
| col2 | Description | `Description` | human label |
| col3 | Type | `XeroType` | **Xero account type enum** (e.g. CURRENT, CURRLIAB, FIXED…) — join key |
| col4 | Vantagepoint Code | `VantagepointCode` | VP **type** code (the value written as VP account Type) |

```python
MAP_ACCOUNT_TYPE_TABLE_NAME = 'map_account_type'
MAP_ACCOUNT_TYPE_COLUMNS = ['Description', 'XeroType', 'VantagepointCode']
MAP_ACCOUNT_TYPE_UNIQUE_COLUMNS = ['XeroType']
```
**Decision needed (see [05](05-open-questions.md) Q-A1):** ship as a **seeded S3 collection** (data-driven, matches Workato — recommended since the file already carries rows) **or** as a static Python constant like QBO's `ACCOUNT_TYPE_MAP`. If kept as a collection, `init_mapping_collections` must seed it from the Workato `data`.

---

## `map_tax_code` — `014-501 PSA Map Tax Code`
Natural/match key: **(XeroName, XeroCode)** = (Xero RateName, ComponentName). Upsert by row id in Workato; fan-out means several rows per Xero rate.

| Workato col | label | Airflow column | notes |
| --- | --- | --- | --- |
| col1 | Xero Name | `XeroName` | Xero TaxRate Name |
| col2 | Xero Code | `XeroCode` | Xero TaxComponent Name |
| col3 | Vantagepoint Code | `VantagepointCode` | generated `X####` |
| col4 | Rate | `Rate` | component rate |
| col5 | Compound On Code | `CompoundOnCode` | base component's VP code |
| col6 | Sequence | `Sequence` | high-water-mark counter |
| col7 | Messages | `Messages` | VP create error |

```python
MAP_TAX_CODE_TABLE_NAME = 'map_tax_code'
MAP_TAX_CODE_COLUMNS = ['XeroName', 'XeroCode', 'VantagepointCode', 'Rate',
                        'CompoundOnCode', 'Sequence', 'Messages']
# One VP tax code per (RateName, ComponentName) — component fan-out.
MAP_TAX_CODE_UNIQUE_COLUMNS = ['XeroName', 'XeroCode']
```

---

## `map_currency_code` — `014-501 PSA Map Currency Code`  (not in the 3 initial-sync recipes; sibling GL `sync_currency_codes`)
Listed for completeness — likely a sibling child DAG.

| col | label | Airflow column |
| --- | --- | --- |
| col1 | Xero Name | `XeroName` |
| col2 | Xero Code | `XeroCode` |
| col3 | Vantagepoint Code | `VantagepointCode` |
| col4 | Messages | `Messages` |

---

## State / tracking tables (for `LOOKUP_TABLE_FLOWS.md`)

### `mapping_table_state` — `014-501 PSA Mapping Table State`
Workato cols: Step, Recipe, Table, Status, Messages, Sequence → Airflow renames (QBO parity): `Recipe`→`DagId`, `Table`→`TableName` (avoid SQL reserved word).
```python
MAPPING_TABLE_STATE_COLUMNS = ['Step', 'DagId', 'TableName', 'Status', 'Messages', 'Sequence']
```
Steps (ordered) for Xero — confirm employee is in scope:
```python
MAPPING_STEPS_ORDERED = [
    ('Map Firms',     MAP_FIRM_TABLE_NAME,              '10'),
    ('Map Employees', MAP_EMPLOYEE_TABLE_NAME,          '20'),  # if employee sync in scope
    ('Map Accounts',  MAP_CHART_OF_ACCOUNTS_TABLE_NAME, '30'),
    ('Map Tax Codes', MAP_TAX_CODE_TABLE_NAME,          '40'),
]
```

### `log` — `014-501 PSA Log`
Cols: Job Payload, Timestamp, Source, Status, Message, History, ParentJobID, Re-Run. In QBO this maps to RAIL log operators rather than a collection — confirm Xero logging strategy (likely the same: use `rail` log ops, not a `log` collection).

---

## Collections to create in `init_mapping_collections` (Xero)
| Collection | UNIQUE key | Seeded? |
| --- | --- | --- |
| `map_firm` | ContactID | no |
| `map_chart_of_accounts` | XeroID | no |
| `map_tax_code` | (XeroName, XeroCode) | no |
| `map_account_type` | XeroType | **yes — from Workato data** (if kept as collection) |
| `mapping_table_state` | — | yes — `seed_mapping_state_rows` |
| `map_employee` | (if in scope) | no |
| `map_currency_code` | XeroCode | (sibling) |

> Confirm final column identifiers against the actual `*.lookup_table.json` `schema[].label` values before coding (this doc is derived from the analysis; treat the JSON as authoritative).
