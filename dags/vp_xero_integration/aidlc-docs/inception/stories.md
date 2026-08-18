# User Stories — Vantagepoint ↔ Xero Initial Mapping Sync (`vp_xero_integration`)

**Epic:** Port the Workato `014-501 PSA` initial mapping sync (firms, chart of accounts, tax codes) to Airflow under `airflow-integrations/dags/vp_xero_integration/mapping_sync`, mirroring the QuickBooks `vp_quickbooks_integration/mapping_sync` reference, using RAIL operators only.

**Reference docs:** [reverse-engineering/xero-mapping-sync/](../reverse-engineering/xero-mapping-sync/README.md) (00–07).
**Conventions:** [00-architecture-parity.md](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md). **Open decisions:** [user-stories-questions.md](user-stories-questions.md) (referenced as Q1–Q10 below).

> Story sizing assumes the QBO `mapping_sync` is cloned as the skeleton; effort is in adapting the source-API layer (Xero) and the `Xero*` field/column identifiers, not inventing the framework.

---

## US-0 — Foundation: package scaffold, shared tables & config
**As** an Integration Developer (P1), **I want** the `vp_xero_integration` package scaffolded with `common/tables.py`, `common/config.py`, `mapping_sync/config.py`, and per-instance files, **so that** all child DAGs share one source of truth for table schemas, connection ids, and instance config.

**Acceptance criteria**
- [ ] Package mirrors QBO layout (`common/`, `mapping_sync/`, `instances/{dev,qa,devops,trial}`, `utils/`, `doc/`) per [00 §1](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md).
- [ ] `common/tables.py` defines Xero table names, column lists, and UNIQUE keys per [04-lookup-tables.md](../reverse-engineering/xero-mapping-sync/04-lookup-tables.md): `map_firm` (UNIQUE `ContactID`), `map_chart_of_accounts` (UNIQUE `XeroID`), `map_tax_code` (UNIQUE `XeroName,XeroCode`), `mapping_table_state`, `map_account_type` (per Q7), `map_employee`/`map_currency_code` per Q1.
- [ ] `IntegrationConfig` sets `S3_INTEGRATION_NAME='vp_xero_integration'`, `DAG_ID_PREFIX='vp_xero_mapping_sync'`, Xero conn-id + conf key (per Q3), VP conn-id unchanged; Variable name prefixes re-namespaced to `vp_xero_*`.
- [ ] `MAPPING_STEPS_ORDERED` = firm/account/tax with sequences 10/20/30 (**employee descoped — Q1=No**).
- [ ] No custom operators introduced (RAIL only; Xero ops via `XeroAPIOperator` / new RAIL ops from US-9).

**Depends on:** US-9 (RAIL Xero pagination), Q7.

---

## US-1 — Scheduled entry & customer fan-out (`main_dag`)
**As** a Support/Ops Engineer (P4), **I want** a scheduled `main_dag` that lists enabled VP↔Xero customers and triggers one dispatcher run per customer, **so that** the initial mapping runs per-tenant on a schedule without manual per-customer triggering.

**Acceptance criteria**
- [ ] `main_dag` (per instance) gets a middleware OAuth token (`SimpleHttpOperator` POST `/api/v1/oauth/token`) and fetches customers (`GET /api/v1/integrations`), filtered to the VP↔Xero integration.
- [ ] `TriggerDagRunForEachItemOperator` triggers the dispatcher once per enabled customer, forwarding `connections`, `customerId`, `company_key`, `integrationType`, `region`, and the middleware `config` block.
- [ ] Schedule defaults to `mapping_population_schedule` (e.g. daily 03:00), overridable per instance via Variable.
- [ ] Only `main_dag` is scheduled; dispatcher + children are `schedule_interval=None`.

**Depends on:** US-0, Q3.

---

## US-2 — Dispatcher: init, state seeding & orchestration (`dispatcher_dag`)
**As** an Implementation Consultant (P2), **I want** a per-customer dispatcher that initialises the mapping collections, seeds step state, runs the child syncs in order, and reports a single pass/fail, **so that** a customer's initial mapping is a one-click, idempotent, observable operation.

**Acceptance criteria**
- [ ] **Init gate**: `is_mapping_init_already_done` reads Variable `vp_xero_mapping_init_{customerId}_{instance}`; if done → skip init, else → `init_mapping_collections`.
- [ ] **`init_mapping_collections`** (one `S3CreateMultiTableCollectionOperator`) creates all collections per [04 §"Collections to create"](../reverse-engineering/xero-mapping-sync/04-lookup-tables.md) with the documented UNIQUE keys; seeds `mapping_table_state` via `seed_mapping_state_rows`; seeds `map_account_type` if kept as a collection (Q7).
- [ ] **`apply_premapping_state`** reads `CFG_UpgradeDataSync` (Q3): `false`→all `Complete` (children skip); `true`→`''` (children run); content-aware override flips empty tables to `''`.
- [ ] **Strict order**: `firm → account_code → tax_code → validate` via `>>` chaining; each child triggered with `wait_for_completion=True` (employee descoped — Q1=No).
- [ ] **Error aggregation**: `GatherResultsFromDagRunsOperator` per child → `combine_child_dag_errors` → `has_sync_errors`; on error → `FailOperator`; on success → `update_last_run_time` → `mark_all_steps_ready` → `mark_mapping_init_complete`.
- [ ] `PostDagRunDetailsToMiddlewareApiOperator` posts run details (`trigger_rule='all_done'`).
- [ ] Re-running after a failure retries from scratch (init flag only set on clean success).

**Depends on:** US-0, Q3, Q7.

---

## US-3 — Firm mapping sync (`map_firm_dag` / `_firm_sync.py`)
**As** a Customer Finance Admin (P3), **I want** Xero contacts cross-referenced to Vantagepoint firms (creating missing firms + addresses), **so that** AP/AR transactions post against the correct firm.

**Scope of logic** (parity with [01-synch-firms.md](../reverse-engineering/xero-mapping-sync/01-synch-firms.md) + seeding [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md)):

**Acceptance criteria**
- [ ] Fetch **all** Xero contacts (active + archived) **with pagination** (Q10; depends on US-9/G1) via `XeroAPIOperator` GET `/Contacts`; flatten addresses (STREET/POBOX) and phone (DDI→DEFAULT).
- [ ] **Seeding merged into the engine (Q2=Yes / QBO parity):** the engine itself anti-joins Xero contacts against existing `map_firm` to find unmapped records — **no separate seed task, no `1900-01-01` sentinel**.
- [ ] Match Xero contact → VP firm by **Name** (`MIN(ClientID)` to collapse duplicates) per Q4; existing → reuse VP `ClientID` (no duplicate); no match → create VP firm (`VantagepointFirmOperator` batch) with `ClientInd=IsCustomer`, `VendorInd=IsSupplier`, Status=A, default Org.
- [ ] Create VP firm addresses (resolve Country/State via VP code tables); **address dedup key** added so re-runs don't duplicate addresses (improvement over Workato, Q-F6).
- [ ] Vendor/Client codes derived from Xero `AccountNumber` (`SL.../PL...`) per Q-F4, populated by the engine (Q-S2 — fix the seeder's blank Vendor/Client gap).
- [ ] Write `map_firm` rows (UNIQUE `ContactID`) via `INSERT OR REPLACE`: FirmID, ContactID, Status, Vendor, Client, XeroName, VantagepointName, ModDate.
- [ ] Errors captured per-record, surfaced to `catch_firm_dag_error`; failures logged via the chosen logging mechanism (Q8); `mark_step_status('Map Firms','Complete')` on success.
- [ ] Idempotent: populated rows skipped; re-runs do not duplicate firms or addresses.

**Depends on:** US-0, US-2, US-9 (pagination), Q4, Q8, Q10.

---

## US-4 — Chart of Accounts mapping sync (`map_account_code_dag` / `_account_sync.py`)
**As** a Customer Finance Admin (P3), **I want** the Xero chart of accounts cross-referenced to Vantagepoint accounts (with correct VP account types), **so that** GL postings map correctly.

**Scope of logic** (parity with [02-synch-accounts.md](../reverse-engineering/xero-mapping-sync/02-synch-accounts.md) + seeding [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md)):

**Acceptance criteria**
- [ ] Fetch Xero accounts (`XeroAPIOperator` GET/search `/Accounts`, filter ACTIVE; exclude `Type='BANK'`) and VP chart of accounts; stage into collections. Seeding merged into the engine (Q2=Yes).
- [ ] Translate Xero account `Type` → VP type via `map_account_type` (Q7); handling for Xero types not in the map per Q-S3 (do **not** silently drop unless confirmed).
- [ ] Compile (Xero-primary LEFT JOIN) per the step-14 SQL; decide add-mapping / create-VP-account / update-VP-account per row.
- [ ] Create VP accounts (`VantagepointChartOfAccountsOperator`) with `Account=XeroCode`, `Name` truncated, `Type=mapped VP type`, Status from Xero; respect `system_formats` AccountLength (Q-A3).
- [ ] Write `map_chart_of_accounts` rows (UNIQUE `XeroID`): Xero Code/Name/Type, VP Code/Name/Type, XeroID, Messages.
- [ ] **Orphan deactivation** (orchestrator behaviour) per Q6: deactivate VP accounts not represented in the mapping — scoped per the decision.
- [ ] Errors captured → `catch`; `mark_step_status('Map Accounts','Complete')`; idempotent re-runs (existing → update, new → create).

**Depends on:** US-0, US-2, Q6, Q7, Q-S3.

---

## US-5 — Tax Code mapping sync (`map_tax_code_dag` / `_tax_code_sync.py`)
**As** a Customer Finance Admin (P3), **I want** Xero tax rates (and their components) cross-referenced to Vantagepoint tax codes (with compound-tax linking), **so that** tax on invoices/vouchers posts correctly.

**Scope of logic** (parity with [03-sync-tax-codes.md](../reverse-engineering/xero-mapping-sync/03-sync-tax-codes.md) + seeding [06](../reverse-engineering/xero-mapping-sync/06-lookup-table-seeding.md)):

**Acceptance criteria**
- [ ] Fetch Xero tax rates (`XeroAPIOperator` GET `/TaxRates`) with nested `TaxComponents[]`; **flatten** to one row per ACTIVE rate × component (port `FlattenTaxRates`); compute `isNewOrUpdated` for early-exit. Seeding merged into the engine (Q2=Yes).
- [ ] Compile (Xero-primary) per the step-16 SQL, **with the OR-precedence bug fixed** (explicit parentheses, Q-T1).
- [ ] For new rows: generate VP code `X####` (Sequence high-water-mark), create VP tax code (`VantagepointTaxCodesOperator`) with Description, Rate, ReverseCharge; write `map_tax_code` (UNIQUE `XeroName,XeroCode`). For existing rows: update Rate only when changed.
- [ ] **Compound linking** second pass: set the compound component's VP `CompoundOnTaxCode` to the base component's VP code; write `CompoundOnCode` (col5).
- [ ] **Fan-out** preserved: one Xero rate → multiple VP tax codes (one per component).
- [ ] Per-row create errors written to `Messages` (col7); `catch`; `mark_step_status('Map Tax Codes','Complete')`; idempotent (no-op early exit; stable generated codes).
- [ ] Tax-seeder `rows.first` bug **not** replicated — iterate per row (Q-S4).

**Depends on:** US-0, US-2, Q-T1, Q-S4.

---

## US-6 — Mapping validation (`validate_mappings_dag` / `_validate.py`)
**As** a Support/Ops Engineer (P4), **I want** post-sync validation of all mapping tables with a clear pass/fail and actionable messages, **so that** I can trust an initial mapping or quickly find what's broken.

**Scope of logic** (parity with [07-validation.md](../reverse-engineering/xero-mapping-sync/07-validation.md)):

**Acceptance criteria**
- [ ] Runs as phase 5 (after firm/[employee]/account/tax), read-only, in one S3 open.
- [ ] **Firm**: flag `map_firm` rows whose ContactID has no live Xero contact, and whose FirmID has no live VP firm. (Archived-contact cleanup handled in the firm sync per Q5.)
- [ ] **Account**: flag rows whose VantagepointCode has no live VP account and whose XeroCode/XeroID has no live Xero account. (col6/col7 self-heal handled in account sync per Q5.)
- [ ] **Tax**: flag rows whose VantagepointCode has no live VP tax code and whose (XeroName,XeroCode) has no live ACTIVE Xero rate/component.
- [ ] On failure: mark `mapping_table_state.Status='Error'` for the failing table and hard-fail the dispatcher (QBO behaviour, Q5) — **not** the Workato silent-return behaviour; fix the firm return bug (Q-V1).
- [ ] Optional duplicate/coverage checks per Q-V4 (surfaced explicitly if added).
- [ ] Validation is idempotent and side-effect-free (beyond state signaling).

**Depends on:** US-0, US-3, US-4, US-5, Q5.

---

## US-7 — Employee mapping sync — **DESCOPED (Q1 = No)**
Employee mapping (`map_employee`, Workato `synch_employees`) is **out of scope** for this effort. `MAPPING_STEPS_ORDERED` is firm/account/tax only (10/20/30). If revived later, it mirrors the QBO `map_employee_dag` adapted to Xero contacts and needs a `synch_employees` recipe analysis first.

---

## US-9 — RAIL Xero operator enablement *(separate, in `replicon-airflow-library`)*
**As** an Integration Developer (P1), **I want** RAIL's Xero support extended to cover everything the integration needs, **so that** `vp_xero_integration` (and future GL/polling efforts) can call Xero entirely through RAIL operators (no custom operators).

**Context:** RAIL already ships a generic `rail/operators/xero_internal/XeroAPIOperator` (GET/POST any endpoint, `filters`/`modified_since`, 429 retry) + `XeroHook`. Full usage inventory and gap analysis: [08-xero-api-inventory.md](../reverse-engineering/xero-mapping-sync/08-xero-api-inventory.md).

**Acceptance criteria**
- [ ] **G1 — Pagination (HIGH, blocks mapping_sync):** `XeroAPIOperator` (or a paginating variant) loops Xero pages for list GETs so `/Contacts` (and other lists) return the full set. Verified against a >1-page tenant.
- [ ] **G2 — PUT support (MEDIUM, GL only):** allow `PUT` for `/CreditNotes/{id}/Allocations` and `/Currencies` (extend `ALLOWED_METHODS` + `XeroHook`, or add typed ops). Not required for mapping_sync.
- [ ] **G3 — Typed operators (LOW, optional):** consider `XeroContactOperator`, `XeroAccountOperator`, `XeroTaxRateOperator`, `XeroInvoiceOperator`, `XeroCreditNoteOperator`, `XeroManualJournalOperator`, `XeroPaymentOperator`, `XeroCurrencyOperator` for parity with the QBO typed-operator set.
- [ ] **G4 — Change-feed trigger/sensor (LOW, polling DAGs):** reusable Xero `updated_*` (modified-since) poll helper for the polling DAGs (out of mapping_sync scope).
- [ ] Each item references the recipes/endpoints in [08-xero-api-inventory.md](../reverse-engineering/xero-mapping-sync/08-xero-api-inventory.md).

**Note on scope:** mapping_sync (firm/account/tax — all GET reads) only hard-depends on **G1**. G2–G4 serve the broader GL/polling integration and can be sequenced separately.

**Blocks:** US-3 (and any list-read). **Owner:** RAIL team.

---

## US-8 — Documentation parity (`mapping_sync/doc/`)
**As** an Integration Developer (P1), **I want** the Xero `mapping_sync/doc/` to carry the equivalent of QBO's `LOOKUP_TABLE_FLOWS.md` + per-table fix logs, **so that** the integration is maintainable and the Workato source-of-truth is traceable.

**Acceptance criteria**
- [ ] `doc/LOOKUP_TABLE_FLOWS.md` reproduces the QBO section template ([00 §7](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md)) for the Xero state/tracking/static tables.
- [ ] `doc/README.md` indexes docs; fix-logs follow Symptom→Root cause→Fix→Workato reference→Code touchpoints.

**Depends on:** US-0.

---

## Story map / sequence

```
US-9 RAIL Xero ops (G1 pagination) ──blocks──► US-3
US-0 Foundation
  └─ US-1 main_dag ──┐
  └─ US-2 dispatcher ┤
       ├─ US-3 firm ─┼─► US-6 validation
       ├─ US-4 account
       └─ US-5 tax
  └─ US-8 docs
(US-7 employee = DESCOPED, Q1=No)
```

## Traceability
| Story | Recipe(s) | Parity doc |
| --- | --- | --- |
| US-2 | premapping / populate_mapping_state (orchestration) | 00, 04 |
| US-3 | synch_firms + map_firms (seed) | 01, 06 |
| US-4 | synch_accounts + sync_accounts + map_accounts (seed) | 02, 06 |
| US-5 | sync_tax_codes + map_tax_codes (seed) | 03, 06 |
| US-6 | validate_firm/account/tax_map | 07 |
| US-7 | — (DESCOPED) | — |
| US-9 | all Xero API calls (RAIL operators) | 08 |
