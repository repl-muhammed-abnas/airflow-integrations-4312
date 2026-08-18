# Code Structure — Xero Migration Slice

## Source: `integration_vantagepoint_xero/`

```
integration_vantagepoint_xero/
├── README.md                                  # Integration feature/mapping overview (business)
├── integrations_014-501 Vantagepoint and Xero.yml   # Azure DevOps build pipeline
└── code/
    ├── readme.md                              # "Vantagepoint - Xero" integration description
    ├── package.manifest                       # name, version 0.0.30, SKU 910-11889, CFG_* props
    ├── 014_501_psa_*.lookup_table.json        # 13 lookup tables (schema; 2 with data)
    └── 014-501 PSA/                           # 71 recipes across 9 functional folders
        ├── Connections/        (2)
        ├── Contacts/           (4)
        ├── Deployment/         (11)
        ├── GL/                 (20)
        ├── Logging/            (3)
        ├── Mapping/            (5 + Initial Synch/ + Lookup Tables/ + Validation/)
        ├── Platform Monitoring/(3)
        ├── Triggers - Polling/ (4)
        └── Triggers - Realtime/(6)
```

## Target: `airflow_mapping_framework/`

```
airflow_mapping_framework/
├── README.md, INTEGRATION_COMPATIBILITY.md
├── config/requirements.txt
├── database/
│   ├── schema.sql                  # customers, mapping_configs, mapping_tables,
│   │                               # mapping_validations, mapping_audit_log,
│   │                               # mapping_cache, regional_configs (+indexes, triggers)
│   └── setup_security.sql          # RLS / tenant security
├── core/
│   ├── mapping_engine.py           # MappingEngine, MappingResult, CacheStrategy
│   ├── tenant_manager.py           # TenantManager, CustomerContext, CustomerScopedSession
│   ├── cache_manager.py            # CacheManager, MemoryCache
│   └── regional_processor.py       # RegionalProcessor (US/UK/CA)
├── operators/
│   ├── mapping_operators.py        # MappingResolverOperator, BulkMappingResolverOperator
│   ├── tenant_operators.py         # Tenant-scoped resolver/validation/manager operators
│   └── integration_operators.py    # IntegrationSetupOperator, Workato(Bulk)ImportOperator
├── utils/workato_importer.py       # WorkatoLookupTableImporter (JSON → DB)
├── validation/mapping_validator.py # MappingValidator (9+ rule types)
├── security/tenant_security.py
├── ui/app.py                       # Flask UI
└── examples/                       # customer_sync_dag.py, integration_migration_example.py, ...
```

## Existing migration tooling (repo root)

```
C:/Workspaces/unionpoint/
├── migrate_workato_data.py         # QBO 014-503 CSV → PostgreSQL (psycopg2). NO customer_id.
├── validate_workato_migration.py   # QBO 014-503: validate + generate migration SQL/operators
├── validate_workato_basic.py       # QBO 014-503: CSV analysis + complexity scoring
├── validate_workato_simple.py      # QBO 014-503: stdlib-only variant of basic
├── demonstrate_table_handling.py   # QBO 014-503: educational demo of table shapes
├── workato_validation_results.json # Output of the QBO validators (13 tables analyzed)
└── mappingTables/                  # 15 CSV files, ALL named lookup_table_data_014-503-psa-*.csv
```

> **Gap:** `mappingTables/` contains **only QuickBooks 014-503 CSVs**. There are **no Xero 014-501 CSVs**, and the root scripts have **no 014-501 table definitions**. The Xero source data lives in `integration_vantagepoint_xero/code/*.lookup_table.json` (JSON, mostly schema-only).
