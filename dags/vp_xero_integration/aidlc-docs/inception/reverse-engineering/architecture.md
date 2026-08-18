# Architecture — Xero Migration Slice

## High-Level Flow (Current vs. Target)

```
  SOURCE (Workato)                MIGRATION TOOLING              TARGET (Airflow Platform)
  ----------------                -----------------              -------------------------
  014-501 PSA Xero                migrate_workato_data.py        airflow_mapping_framework
  integration                     (QBO/014-503 only today)         |
    |                                   |                           +-- PostgreSQL schema
    +-- 13 lookup tables  ---read--->   |  ---INSERT--->            |     mapping_configs
    |     (*.lookup_table.json)         |                           |     mapping_tables
    |                             validate_workato_*.py             |     mapping_validations
    +-- 71 recipes                (QBO/014-503 only today)          |     mapping_audit_log
    +-- 2 connections                                              |     mapping_cache
    +-- package.manifest          utils/workato_importer.py        +-- core engine
                                  (declares vantagepoint_xero)      +-- operators (Airflow)
                                                                    +-- validation
                                                                    +-- Flask UI
```

## Source Architecture (Workato `014-501 PSA`)

- **Format:** Workato package export. Recipes are `*.recipe.json`; lookup tables are `*.lookup_table.json` with `{ name, schema[], (optional) data }`.
- **Recipe folders** (71 recipes total): Connections (2), Contacts (4), Deployment (11), GL (20), Logging (3), Mapping (5 + Initial Synch/Lookup Tables/Validation subfolders), Platform Monitoring (3), Triggers - Polling (4), Triggers - Realtime (6).
- **Lookup tables (13):** map_account_type, map_bank_code, map_chart_of_accounts, map_currency_code, map_employee, map_firm, map_tax_code, outstanding_employee_expenses, outstanding_purchase_invoices, integration_recipes, log, deployment_state, mapping_table_state.
- **Data presence:** Most lookup-table JSON files contain **schema only** (runtime-populated). Only `map_account_type` (~16 rows) and `integration_recipes` (~42 rows) ship with embedded data.

## Target Architecture (Airflow Mapping Framework)

- **Database (PostgreSQL):** multi-tenant schema. Core tables: `customers`, `mapping_configs`, `mapping_tables`, `mapping_validations`, `mapping_audit_log`, `mapping_cache`, `regional_configs`. Every customer-scoped table carries `customer_id` and a unique constraint including `customer_id`.
- **Core engine (`core/`):** `mapping_engine.py` (resolution + caching), `tenant_manager.py` (customer isolation), `cache_manager.py` (memory/Redis/DB/hybrid), `regional_processor.py` (US/UK/CA rules).
- **Operators (`operators/`):** mapping, tenant-scoped, and integration operators including `WorkatoImportOperator` / `BulkWorkatoImportOperator`.
- **Import utility (`utils/workato_importer.py`):** parses Workato lookup JSON (`{name, schema, data:"CSV string"}`), standardizes table names, auto-detects source/target systems, and writes `mapping_configs` + `mapping_tables` with `customer_id` + `region`. Declares a `vantagepoint_xero` integration profile.
- **Validation (`validation/mapping_validator.py`):** required/regex/range/length/type/conditional/unique/FK/business-rule/custom checks.
- **UI (`ui/app.py`):** Flask + SQLAlchemy + WTForms editor for configs, entries, and validation rules.

## Two Migration Paths Observed (a key design tension)

1. **Root CSV scripts** (`migrate_workato_data.py`): read **CSV** files from `mappingTables/`, hardcoded to **QuickBooks 014-503**, write directly via `psycopg2`. Schema used has **no `customer_id`** in the INSERTs (older/simpler schema assumption).
2. **Framework importer** (`utils/workato_importer.py`): reads **Workato JSON** directly, multi-tenant aware (`customer_id` + `region`), declares Xero support.

> These two paths assume **different input formats (CSV vs JSON)** and **different schema shapes (no `customer_id` vs `customer_id`)**. Reconciling them is central to any Xero migration work — see code-quality-assessment.md.
