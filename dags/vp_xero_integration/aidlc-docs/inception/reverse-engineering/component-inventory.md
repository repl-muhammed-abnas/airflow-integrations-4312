# Component Inventory — Xero Migration Slice

## Source Lookup Tables (014-501 PSA) — the data to migrate

| Lookup table | Display name | Columns (labels) | Direction | Data present? |
| --- | --- | --- | --- | --- |
| map_account_type | 014-501 PSA Map Account Type | Description (Not Used), Description, Type, Vantagepoint Code | Xero→Vp | **Yes (~16 rows)** |
| map_bank_code | 014-501 PSA Map Bank Code | Vantagepoint name, Vantagepoint code, Xero name, Xero ID, Status, Company, Org, Account | n/a (manual) | Schema only |
| map_chart_of_accounts | 014-501 PSA Map Chart of Accounts | Xero Code, Xero Name, Xero Type, Vantagepoint Code, Vantagepoint Name, Vantagepoint Type, Xero ID, Messages | Xero→Vp | Schema only |
| map_currency_code | 014-501 PSA Map Currency Code | Xero Name, Xero Code, Vantagepoint Code, Messages | Xero→Vp | Schema only |
| map_employee | 014-501 PSA Map Employee | Employee, ContactID, Status, AccountNumber, CreatedDate, ModDate, Messages | Vp→Xero | Schema only |
| map_firm | 014-501 PSA Map Firm | Firm ID, Contact ID, Status, Vendor, Client, Xero Name, Vantagepoint Name, Mod Date | Xero↔Vp | Schema only |
| map_tax_code | 014-501 PSA Map Tax Code | Xero Name, Xero Code, Vantagepoint Code, Rate, Compound On Code, Sequence, Messages | Xero→Vp | Schema only |
| outstanding_employee_expenses | 014-501 PSA Outstanding Employee Expenses | Period, Post Seq, Employee, Org, Transaction Date, Voucher, Outstanding Amount, Invoice ID, Messages | runtime state | Schema only |
| outstanding_purchase_invoices | 014-501 PSA Outstanding Purchase Invoices | Batch, Voucher, WBS1, WBS2, WBS3, Line_Amount, Outstanding_Amount, Account, Org, InvoiceID | runtime state | Schema only |
| integration_recipes | 014-501 PSA Integration Recipes | Id, Name, Sequence, Folder Id | platform meta | **Yes (~42 rows)** |
| log | 014-501 PSA Log | Job Payload, Timestamp, Source, Status, Message, History, ParentJobID, Re-Run | runtime log | Schema only |
| deployment_state | 014-501 PSA Deployment State | Step, Sequence, Recipe, Status, Messages, Completed Date/Time, Recipe ID | runtime state | Schema only |
| mapping_table_state | 014-501 PSA Mapping Table State | Step, Recipe, Table, Status, Messages, Sequence | runtime state | Schema only |

**Classification for migration:**
- **Reference/master mapping data** (candidates to migrate as mappings): map_account_type, map_bank_code, map_chart_of_accounts, map_currency_code, map_employee, map_firm, map_tax_code.
- **Runtime/operational state** (likely NOT mapping data — logs, outstanding balances, deployment state): outstanding_employee_expenses, outstanding_purchase_invoices, log, deployment_state, mapping_table_state.
- **Platform metadata:** integration_recipes.

## Target Framework Tables (destination)

| Table | Key columns | Role |
| --- | --- | --- |
| customers | id, customer_code (UNIQUE), tenant_id (UUID) | Tenant registry |
| mapping_configs | id, customer_id, table_name, display_name, source_system, target_system, region, is_active — UNIQUE(customer_id, table_name, region) | One row per logical lookup table |
| mapping_tables | id, customer_id, config_id, source_key, source_value, target_key, target_value, mapping_type, priority, conditions(JSONB), metadata(JSONB), region — UNIQUE(customer_id, config_id, source_key, region) | The migrated rows |
| mapping_validations | id, customer_id, config_id, validation_type, validation_rule(JSONB), error_message | Per-config rules |
| mapping_audit_log | customer_id, action, old_values, new_values, changed_by | Auto-trigger audit |
| mapping_cache | cache_key (PK), customer_id, cache_value(JSONB), expires_at | Resolution cache |
| regional_configs | region, config_key, config_value(JSONB) — UNIQUE(region, config_key) | Shared region settings |

## Migration / Validation Tooling Components

| Component | Path | Coverage | Input | Output |
| --- | --- | --- | --- | --- |
| WorkatoDataMigrator | migrate_workato_data.py | QBO 014-503 | CSV in `mappingTables/` | INSERT into mapping_configs/mapping_tables (no customer_id) |
| WorkatoMigrationValidator | validate_workato_migration.py | QBO 014-503 | CSV | report JSON, migration SQL, operator examples |
| basic/simple validators | validate_workato_basic.py, validate_workato_simple.py | QBO 014-503 | CSV | console + workato_validation_results.json |
| demonstrate_table_handling.py | (root) | QBO 014-503 | inline samples | console demo |
| WorkatoLookupTableImporter | airflow_mapping_framework/utils/workato_importer.py | declares vantagepoint_xero | **Workato JSON** | mapping_configs/mapping_tables (customer_id + region) |

> The only component that natively understands **Workato JSON** and is **tenant-aware** is `workato_importer.py`. The root scripts are **CSV + QBO-only**.
