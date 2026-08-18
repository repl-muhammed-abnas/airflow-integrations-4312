# Units of Work — Vantagepoint ↔ Xero Initial Mapping Sync

Formalizes the units from the [execution plan](../plans/execution-plan.md). Component-level design for each unit is in [00-architecture-parity.md](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md) (Application Design was folded in — see execution plan §4).

**Target package:** `airflow-integrations/dags/vp_xero_integration/` (mirrors `vp_quickbooks_integration/`).
**Enabling dependency:** `replicon-airflow-library` (RAIL).

---

## U0 — RAIL Xero pagination
- **Story:** US-9 (G1). G2 (PUT), G3 (typed ops), G4 (trigger) explicitly deferred.
- **Repo/area:** `replicon-airflow-library/rail/rail/operators/xero_internal/`
- **Scope:** Add page-looping to `XeroAPIOperator` (or a paginating variant) so list GETs return all pages; verify against a >1-page tenant. Keep 429 retry + `modified_since`.
- **Spec:** [08 §3 G1](../reverse-engineering/xero-mapping-sync/08-xero-api-inventory.md).
- **Construction stages:** Functional Design SKIP · NFR light · Infra SKIP · Code Gen ✔ · Build&Test ✔.
- **Owner:** RAIL team. **Depends on:** —. **Blocks:** U2 (and any list read).

## U1 — Foundation + orchestration
- **Stories:** US-0, US-1, US-2.
- **Files:** `common/{tables.py,config.py,python_callable_method.py,main_dag.py,instances/*}`, `mapping_sync/{config.py,main_dag.py,dispatcher_dag.py,instances/*,utils/_shared.py,utils/python_callable_method.py}`.
- **Scope:** Package scaffold; single-source `tables.py` (column lists + UNIQUE keys per [04](../reverse-engineering/xero-mapping-sync/04-lookup-tables.md)); `IntegrationConfig` (Xero conn-id/prefixes); scheduled `main_dag` customer fan-out; `dispatcher_dag` (init gate → `init_mapping_collections` incl. seeded `map_account_type` → `apply_premapping_state` → ordered child triggers → gather-errors → ready/init-complete → post run details). `MAPPING_STEPS_ORDERED` = firm/account/tax (10/20/30).
- **Construction stages:** Functional Design (light) ✔ · **NFR Req/Design (light) ✔** (multi-tenant isolation, idempotency, rate limits, schedule/concurrency — inherited by U2–U5) · Infra SKIP (inherited) · Code Gen ✔ · Build&Test ✔.
- **Depends on:** U0 (for downstream reads). **Blocks:** U2, U3, U4, U6.

## U2 — Firm mapping
- **Story:** US-3.
- **Files:** `mapping_sync/map_firm_dag.py`, `utils/_firm_sync.py`.
- **Scope:** Engine-merged seed+sync ([01](../reverse-engineering/xero-mapping-sync/01-synch-firms.md), [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md)): paginated Xero `/Contacts`; anti-join vs `map_firm`; Name-match (MIN ClientID, Q4=A); create VP firm + addresses (dedup’d); Vendor/Client from AccountNumber; `INSERT OR REPLACE` on UNIQUE `ContactID`. Fix seeder Vendor/Client gap (Q9).
- **Construction stages:** **Functional Design ✔** · NFR inherit (SKIP) · Infra SKIP · Code Gen ✔ · Build&Test ✔.
- **Depends on:** U1, **U0/G1 (hard gate)**. **Blocks:** U5.

## U3 — Account mapping
- **Story:** US-4.
- **Files:** `mapping_sync/map_account_code_dag.py`, `utils/_account_sync.py`.
- **Scope:** Engine-merged ([02](../reverse-engineering/xero-mapping-sync/02-synch-accounts.md), [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md)): Xero accounts (ACTIVE, exclude BANK) + VP CoA; type translate via seeded `map_account_type` collection (Q7=A); compile JOIN; create/update VP accounts (AccountLength guard); UNIQUE `XeroID`. Scoped orphan deactivation (Q6=A). Don't silently drop unmapped types (Q9/Q-S3).
- **Construction stages:** **Functional Design ✔** · NFR inherit · Infra SKIP · Code Gen ✔ · Build&Test ✔.
- **Depends on:** U1. **Blocks:** U5.

## U4 — Tax mapping
- **Story:** US-5.
- **Files:** `mapping_sync/map_tax_code_dag.py`, `utils/_tax_code_sync.py`.
- **Scope:** Engine-merged ([03](../reverse-engineering/xero-mapping-sync/03-sync-tax-codes.md), [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md)): Xero `/TaxRates` → flatten to rate×component; compile (fix OR-precedence, Q-T1); generate `X####` VP codes; create/update VP tax codes; **compound-link** second pass; UNIQUE `(XeroName,XeroCode)`; fan-out preserved. Iterate per-row (fix `rows.first`, Q9/Q-S4).
- **Construction stages:** **Functional Design ✔** · NFR inherit · Infra SKIP · Code Gen ✔ · Build&Test ✔.
- **Depends on:** U1. **Blocks:** U5.

## U5 — Validation
- **Story:** US-6.
- **Files:** `mapping_sync/validate_mappings_dag.py`, `utils/_validate.py`.
- **Scope:** Phase-5 referential checks ([07](../reverse-engineering/xero-mapping-sync/07-validation.md)): firm/account/tax dangling-reference checks; read-only/reporting (self-heal + archived-cleanup live in U2/U3 engines, Q5=A); signal via `mapping_table_state.Status='Error'` + dispatcher hard-fail; fix firm return bug (Q-V1).
- **Construction stages:** Functional Design (light) ✔ · NFR inherit · Infra SKIP · Code Gen ✔ · Build&Test ✔.
- **Depends on:** U2, U3, U4.

## U6 — Documentation parity
- **Story:** US-8.
- **Files:** `mapping_sync/doc/{README.md,LOOKUP_TABLE_FLOWS.md,MAP_*_SYNC_FIX_LOG.md}`.
- **Scope:** Reproduce QBO doc structure ([00 §7](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md)); fix-logs record the Q9 bug fixes.
- **Construction stages:** Functional Design SKIP · NFR SKIP · Infra SKIP · Code Gen ✔ · Build&Test (doc lint).
- **Depends on:** U1.

---

## Summary

| Unit | Stories | FuncDesign | NFR | Infra | CodeGen | Owner |
| --- | --- | --- | --- | --- | --- | --- |
| U0 RAIL pagination | US-9/G1 | skip | light | skip | ✔ | RAIL team |
| U1 Foundation+Orch | US-0,1,2 | light | **light** | skip | ✔ | Integration dev |
| U2 Firm | US-3 | ✔ | inherit | skip | ✔ | Integration dev |
| U3 Account | US-4 | ✔ | inherit | skip | ✔ | Integration dev |
| U4 Tax | US-5 | ✔ | inherit | skip | ✔ | Integration dev |
| U5 Validation | US-6 | light | inherit | skip | ✔ | Integration dev |
| U6 Docs | US-8 | skip | skip | skip | ✔ | Integration dev |
