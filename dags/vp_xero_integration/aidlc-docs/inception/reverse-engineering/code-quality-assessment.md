# Code Quality Assessment — Xero Migration Slice

## Strengths
- The target framework is well-structured: clear separation of engine / operators / validation / UI, multi-tenant schema with audit triggers and caching.
- `workato_importer.py` already models the correct abstraction (Workato JSON → tenant-scoped mapping rows) and declares a `vantagepoint_xero` profile.
- Validators produce useful complexity scoring and data-quality metrics.

## Findings / Risks (ranked)

### 1. Two divergent migration paths with incompatible assumptions — HIGH
- Root `migrate_workato_data.py` consumes **CSV** and writes **without `customer_id`**.
- `utils/workato_importer.py` consumes **Workato JSON** and writes **with `customer_id` + region**.
- The current `schema.sql` requires `customer_id`. **The root script's INSERTs are inconsistent with the current schema** and would fail (or silently target a legacy schema).
- *Impact:* Ambiguous source of truth for the Xero migration. A decision is required on which path to use/extend.

### 2. No Xero (014-501) inputs staged — HIGH
- `mappingTables/` contains **only 014-503 (QuickBooks) CSVs**.
- Root scripts have **no 014-501 table definitions**.
- *Impact:* Even with working tooling, there is no Xero data to migrate until it is exported/staged.

### 3. Xero lookup tables are mostly schema-only — MEDIUM
- 11 of 13 Xero `*.lookup_table.json` files contain **no data rows** (only `map_account_type` and `integration_recipes` have data).
- *Impact:* Real reference data (firm map, chart of accounts, tax codes, etc.) must be sourced from the **live Workato tenant**, not the repo. Migration of *structure* vs *data* are different deliverables.

### 4. Mixing master data with runtime/operational state — MEDIUM
- Several "lookup tables" are actually runtime state (log, deployment_state, mapping_table_state, outstanding_*). These likely should **not** become `mapping_tables` rows.
- *Impact:* A classification/selection step is needed to migrate only true mapping data.

### 5. Column-label inconsistencies — LOW/MEDIUM
- Inconsistent labels across packages (e.g., `Vantagepoint Code` vs `Vantagepoint  code` double-space; `Untitled column N`).
- *Impact:* Source/target column auto-detection must be robust or explicitly configured per table.

### 6. Hardcoded DB credentials & interactive prompt — LOW
- `migrate_workato_data.py` hardcodes `localhost`/`postgres`/`password` and uses `input()`. Not suitable for automated/CI runs.

### 7. Validator/migrator duplication — LOW
- `validate_workato_basic.py` and `validate_workato_simple.py` are near-duplicates. Consolidation opportunity.

## Recommendation for next stages
The requirements stage must resolve: **(a)** which migration path is the source of truth, **(b)** which Xero tables are in scope (master data vs runtime state), **(c)** where the actual Xero data comes from (live Workato export vs repo JSON vs supplied CSV), and **(d)** the target schema variant (multi-tenant `customer_id` vs legacy).
