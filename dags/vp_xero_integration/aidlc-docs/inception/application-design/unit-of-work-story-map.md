# Unit ↔ Story ↔ Source Map

Traceability across units, user stories, personas, Workato source recipes, and parity docs.

| Unit | Stories | Primary persona | Workato source recipe(s) | Parity doc(s) | Target Airflow files |
| --- | --- | --- | --- | --- | --- |
| **U0** RAIL Xero pagination | US-9 (G1) | P1 Integration Dev | n/a (all list reads) | [08](../reverse-engineering/xero-mapping-sync/08-xero-api-inventory.md) | `rail/operators/xero_internal/xero_api_operator.py` |
| **U1** Foundation + Orchestration | US-0, US-1, US-2 | P2 Consultant / P4 Ops | premapping, populate_mapping_state, populate_mapping_table | [00](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md), [04](../reverse-engineering/xero-mapping-sync/04-lookup-tables.md) | `common/*`, `mapping_sync/{config,main_dag,dispatcher_dag}.py`, `utils/_shared.py` |
| **U2** Firm mapping | US-3 | P3 Finance Admin | synch_firms, map_firms | [01](../reverse-engineering/xero-mapping-sync/01-synch-firms.md), [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md) | `mapping_sync/map_firm_dag.py`, `utils/_firm_sync.py` |
| **U3** Account mapping | US-4 | P3 Finance Admin | synch_accounts, sync_accounts, map_accounts | [02](../reverse-engineering/xero-mapping-sync/02-synch-accounts.md), [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md) | `mapping_sync/map_account_code_dag.py`, `utils/_account_sync.py` |
| **U4** Tax mapping | US-5 | P3 Finance Admin | sync_tax_codes (GL), map_tax_codes | [03](../reverse-engineering/xero-mapping-sync/03-sync-tax-codes.md), [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md) | `mapping_sync/map_tax_code_dag.py`, `utils/_tax_code_sync.py` |
| **U5** Validation | US-6 | P4 Ops | validate_firm_map, validate_account_map, validate_tax_map | [07](../reverse-engineering/xero-mapping-sync/07-validation.md) | `mapping_sync/validate_mappings_dag.py`, `utils/_validate.py` |
| **U6** Docs | US-8 | P1 Integration Dev | (all) | [00 §7](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md) | `mapping_sync/doc/*` |

## Story coverage check
- US-0 → U1 · US-1 → U1 · US-2 → U1
- US-3 → U2 · US-4 → U3 · US-5 → U4 · US-6 → U5
- US-8 → U6 · US-9 → U0
- US-7 (employee) → **DESCOPED** (Q1=No); not mapped to any unit.

✅ All in-scope stories map to exactly one unit. No orphan stories; no unit without a story.

## Decision references applied per unit
| Decision | Applied in |
| --- | --- |
| Q1 (no employee) | scope — no employee unit |
| Q2 (merge seeding) | U2, U3, U4 |
| Q3 / G1 (RAIL pagination) | U0 (blocks U2) |
| Q4 (name matching) | U2 |
| Q5 (validation read-only; self-heal in sync) | U5 (+ U2/U3 engines) |
| Q6 (scoped orphan deactivation) | U3 |
| Q7 (account_type seeded collection) | U1 (seed) + U3 (use) |
| Q8 (RAIL logging + run-details) | U1 (+ all) |
| Q9 (fix Workato bugs) | U2, U3, U4, U5 (+ fix-logs in U6) |
| Q10 (paginate) | U0, U2 (+ U3/U4 when available) |
