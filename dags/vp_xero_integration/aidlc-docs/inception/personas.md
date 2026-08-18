# Personas — Vantagepoint ↔ Xero Initial Mapping Sync (Airflow)

| ID | Persona | Description | Goals | Pain points the mapping sync addresses |
| --- | --- | --- | --- | --- |
| **P1** | **Integration Developer** (Deltek/UnionPoint engineering) | Builds and maintains the `vp_xero_integration` Airflow DAGs using RAIL operators. | Reuse the proven QBO `mapping_sync` pattern; achieve Workato parity; keep code maintainable and multi-tenant. | Workato recipes are opaque/hard to test; needs a clear parity spec and a per-table structure that mirrors QBO. |
| **P2** | **Implementation / Onboarding Consultant** | Onboards a customer onto the VP↔Xero integration; runs the one-time initial mapping. | Cross-reference tables (firms, accounts, tax codes) populated correctly the first time; clear pass/fail signal. | Manual mapping is error-prone; partial/failed seeding is hard to diagnose. |
| **P3** | **Customer Finance Admin** (uses VP + Xero) | Relies on accurate GL/firm/tax mappings so transactions flow correctly between systems. | Firms, chart of accounts, and tax codes map correctly; no duplicates; archived records handled. | Mis-mapped accounts/tax codes cause posting errors; duplicate firms. |
| **P4** | **Support / Operations Engineer** | Monitors scheduled runs, triages failures, re-runs syncs. | Idempotent re-runs; actionable error messages; per-step status visibility; alerts on failure. | Silent failures; no run summary; unclear which mapping step failed. |

## Notes
- The mapping sync is **operator/back-office facing**, not an end-user UI feature. P1/P2/P4 are the primary actors; P3 is the indirect beneficiary (data correctness).
- Multi-tenancy: every run is scoped to one customer (`customerId`/`company_key`); P2/P4 operate across many customers via the scheduled `main_dag` fan-out.
