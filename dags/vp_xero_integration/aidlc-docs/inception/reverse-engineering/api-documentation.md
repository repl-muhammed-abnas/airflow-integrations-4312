# API / Interface Documentation — Xero Migration Slice

> No REST API is exposed for the migration itself. The relevant "interfaces" are the programmatic entry points and data contracts used to move Xero lookup data into the framework.

## Framework importer interface (`utils/workato_importer.py`)

`WorkatoLookupTableImporter`
- `import_from_workato_json(workato_file_path, customer_code, integration_name, region)` — import a single Workato lookup-table JSON file.
- `bulk_import_integration(integration_path, customer_code, integration_name, region)` — import every lookup table under an integration directory.
- `create_integration_summary(integration_path)` — analyze an integration before import (table list, row counts).

**Expected input contract (Workato JSON):**
```json
{
  "name": "014-501 PSA Map Chart of Accounts",
  "schema": [ { "name": "col1", "label": "Xero Code", "sticky": true }, ... ],
  "data": "<CSV-formatted string of rows>"   // NOTE: Xero files in repo usually omit this
}
```
Helper logic: `_parse_workato_table_structure`, `_standardize_table_name`, `_determine_systems`, `_get_or_create_customer`, `_create_mapping_config`, `_import_mapping_data`.

## Root migrator interface (`migrate_workato_data.py`)

`WorkatoDataMigrator(db_config)`
- `connect_database()`, `migrate_all_tables()`, `migrate_table_data(table_name, config, mapping_dir='mappingTables')`, `create_mapping_config(...)`, `add_validation_rules(...)`, `generate_test_queries()`.

**Expected input contract (CSV):** files named `lookup_table_data_014-503-psa-*.csv` in `mappingTables/`, with per-table `source_column` / `target_column` defined in the hardcoded `table_configs` dict (QBO only).

**DB write contract (current root script):**
```sql
INSERT INTO mapping_configs (table_name, display_name, description,
  source_system, target_system, region, is_active, created_by) ...
INSERT INTO mapping_tables (config_id, source_key, source_value, target_key,
  target_value, mapping_type, priority, metadata, region, is_active, created_by) ...
```
> Note: omits `customer_id`, which the current `schema.sql` defines as a NOT-NULL FK on both tables.

## Airflow operator interfaces (consumption, post-migration)
- `MappingResolverOperator`, `BulkMappingResolverOperator` — resolve mappings in DAGs.
- `TenantMappingResolverOperator`, `TenantBulkMappingResolverOperator`, `TenantDataValidationOperator`, `TenantMappingTableManagerOperator` — tenant-scoped variants.
- `WorkatoImportOperator`, `BulkWorkatoImportOperator`, `IntegrationSetupOperator` — run imports as Airflow tasks.

## Mapping resolution contract (`MappingEngine.resolve_mapping`)
`resolve_mapping(table_name, source_key, region, context) -> MappingResult{ success, mapped_value, mapping_type, metadata, errors }`.
