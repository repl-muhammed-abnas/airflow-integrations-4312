# Dependencies — Xero Migration Slice

## Runtime / external dependencies
- **PostgreSQL** database (target store). Schema defined in `airflow_mapping_framework/database/schema.sql`. Multi-tenant; requires `customers` rows before mapping data.
- **Apache Airflow** (for operator/DAG consumption of mappings — not required for the migration step itself).
- **Redis** (optional, for hybrid cache strategy).
- **Python packages:** `psycopg2` (DB), Flask/SQLAlchemy/WTForms (UI), plus packages in `airflow_mapping_framework/config/requirements.txt`.

## Inter-component dependencies (migration direction)

```
Xero lookup JSON ──► [Importer/Migrator] ──► mapping_configs ──► mapping_tables
   (source data)                                   ▲                  │
                                            customers (tenant)        ▼
                                                              mapping_validations
                                                              mapping_audit_log (auto)
```

- `mapping_tables` rows require an existing `mapping_configs.id` (`config_id` FK).
- Both require an existing `customers.id` (`customer_id` FK) in the multi-tenant schema.
- The **root `migrate_workato_data.py` INSERTs omit `customer_id`** — it targets an older/simpler schema variant. Running it against the current multi-tenant `schema.sql` would fail or require a default customer. **This mismatch must be resolved for Xero migration.**

## Data dependencies / prerequisites
- Xero source data largely **does not exist as exportable rows** in the repo: 11 of 13 lookup tables are schema-only. Real Xero mapping data would need to be **exported from the live Workato tenant** (or supplied as CSV/JSON) before migration of actual values.
- `mappingTables/` currently holds **only QuickBooks 014-503 CSVs** — no Xero inputs are staged.

## Known coupling risks
- Column-label drift between Xero (014-501) and QuickBooks (014-503) tables (e.g., "Vantagepoint Code" vs "Vantagepoint  code" with double space) — source/target column auto-detection must handle this.
- Two divergent migration code paths (CSV root scripts vs JSON framework importer) create ambiguity about the source of truth.
