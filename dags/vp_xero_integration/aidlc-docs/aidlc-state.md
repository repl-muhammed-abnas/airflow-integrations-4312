```yaml
project:
  name: "UnionPoint Integration Platform"
  type: "brownfield"
  workspace_root: "C:/Workspaces/unionpoint"

current_stage: "code_generation"
current_phase: "construction"
current_unit: "ALL UNITS U0-U6 code-complete (awaiting dev-airflow build&test + approval + commit)"

scope:
  goal: "Migration / data work"
  focus_subproject: "integration_vantagepoint_xero (014-501 PSA)"
  active_work: "Port the Xero Initial Mapping Sync (firms, accounts, tax codes) to Airflow at airflow-integrations/dags/vp_xero_integration, mirroring QBO mapping_sync"

inception:
  workspace_detection: "complete"
  requirements_analysis: "complete (captured via reverse-engineering parity docs 00-08 + stories.md/user-stories-questions.md; no standalone requirements.md by user-driven flow)"
  reverse_engineering: "complete (approved 2026-06-24)"
  reverse_engineering_artifacts:
    - "inception/reverse-engineering/ (9 slice docs)"
    - "inception/reverse-engineering/xero-mapping-sync/ (9 parity docs: README, 00-07)"
  user_stories: "complete (approved 2026-06-24; Q1/Q2/Q3 answered, Q4-Q10 defaulted)"
  workflow_planning: "complete (approved 2026-06-24)"
  application_design: "skipped (folded into reverse-engineering/xero-mapping-sync/00-architecture-parity.md per execution plan)"
  units_generation: "complete (awaiting approval)"
  requirements_analysis: "pending"
  user_stories: "pending"
  workflow_planning: "pending"
  application_design: "pending"
  units_generation: "pending"

construction:
  units:
    - id: "U0"
      name: "RAIL Xero operator enablement"
      scope: "G1 pagination + G2 PUT on XeroAPIOperator (opt-in) + G3 typed convenience operators, mirroring intuit_internal; in replicon-airflow-library/rail/rail/operators/xero_internal"
      functional_design: "skipped (spec in 08-xero-api-inventory.md §3 + intuit_internal as design template)"
      code_generation: "complete (awaiting approval) — 22 unit tests pass (per-operator test files); rail imports clean"
      build_and_test: "unit tests pass; manual dev-airflow run pending (rail edit needs airflow restart)"
    - id: "U1"
      name: "Foundation + orchestration"
      scope: "vp_xero_integration package scaffold mirroring QBO mapping_sync: common/{tables,config,main_dag,python_callable_method,instances}, mapping_sync/{config,main_dag,dispatcher_dag,instances,utils/{_shared,python_callable_method}}. Employee descoped. map_account_type SEEDED (16 rows) in init_mapping_collections."
      functional_design: "light (design = 00-architecture-parity.md; tables = 04-lookup-tables.md + authoritative 014_501 lookup JSONs)"
      code_generation: "complete (awaiting approval) — all modules compile + leaf modules import clean; DAG parse pending dev-airflow"
      build_and_test: "py_compile + import smoke pass; full DAG parse via dev-airflow pending; no unit tests yet (engines land U2-U5)"
    - id: "U2"
      name: "Firm mapping"
      scope: "map_firm_dag.py + utils/_firm_sync.py. Engine-merged seed+sync (docs 01+06, Option A): paginated XeroContactOperator (active+archived) → anti-join vs map_firm by ContactID → name-match MIN(ClientID) reuse (Q4=A) or create VP firm + STREET/POBOX addresses (deduped, country/state via codetable) → upsert keyed ContactID. Vendor/Client parsed from AccountNumber + ClientInd/VendorInd from IsCustomer/IsSupplier (fixes Workato seeder gap, Q9). Shim _firm_sync block uncommented."
      functional_design: "done (engine logic per docs 01/06; VP operator signatures verified in rail)"
      code_generation: "complete (awaiting approval) — compile + import clean; 15 firm-helper unit tests pass"
      build_and_test: "py_compile + import + 15 unit tests (pure helpers) pass; full-engine integration (VP create/address, codetable names FW_CFGCountry/CFGStates) pending dev-airflow"
      depends_on: "U1, U0/G1 (hard gate for paginated contacts)"
    - id: "U3"
      name: "Account mapping"
      scope: "map_account_code_dag.py + utils/_account_sync.py. Engine-merged (docs 02+06): XeroAccountOperator(list) + VP CoA → staged run-local collections → COMPILE_ACCOUNT_CODES_SQL (Xero-primary, WHERE Type!=BANK) → foreach match/create/update VP accounts (AccountLength guard from system_formats; Name[:39]); type translate via SEEDED map_account_type S3 collection (Q7=A); unmapped types surfaced in Messages not dropped (Q9/Q-S3); idempotent upsert keyed XeroID; scoped orphan deactivation of previously-Xero-sourced VP accounts only (Q6=A). Shim _account_sync block uncommented."
      functional_design: "done (engine logic per doc 02; VP CoA/system_formats operator signatures verified in rail)"
      code_generation: "complete (awaiting approval) — compile + import clean; 8 account-helper unit tests pass (23 total with firm)"
      build_and_test: "py_compile + import + unit tests pass; full-engine integration (VP CoA create/update/deactivate, system_formats AccountLength, run-local compile JOIN) pending dev-airflow"
      depends_on: "U1"
    - id: "U4"
      name: "Tax mapping"
      scope: "map_tax_code_dag.py + utils/_tax_code_sync.py. Engine-merged (docs 03+06): XeroTaxRateOperator(list) → flatten_xero_tax_rates (1 row per ACTIVE rate × component) → staged collections → COMPILE_TAX_CODES_SQL (Xero-primary; OR-precedence FIXED with parens Q-T1; compound RateName#ComponentName subquery) → two-pass engine: pass1 reuse/generate X#### code + create/rate-update VP tax codes (ReverseCharge flag; Sequence high-water-mark); pass2 compound-link base VP code via CompoundOnTaxCode; batched upsert keyed (XeroName,XeroCode); fan-out preserved; per-row iteration (fixes Workato rows.first bug Q9/Q-S4). No tax-group step (Xero has none). Shim _tax_code_sync block uncommented (TAX_GROUP_IDS_SQL dropped — no Xero analogue)."
      functional_design: "done (engine logic per doc 03; VP tax_codes operator signature verified in rail)"
      code_generation: "complete (awaiting approval) — compile + import clean; 16 tax-helper unit tests pass (39 total across firm/account/tax)"
      build_and_test: "py_compile + import + unit tests pass; full two-pass engine integration (VP tax create/update/compound PUT, run-local compile JOIN) pending dev-airflow"
      depends_on: "U1"
    - id: "U5"
      name: "Validation"
      scope: "validate_mappings_dag.py + utils/_validate.py. Phase-5 READ-ONLY referential anti-joins (doc 07): map_firm ContactID→live Xero contact + FirmID→live VP firm (ARCHIVED rows skipped); map_chart_of_accounts VantagepointCode→live VP account + XeroID→live Xero account; map_tax_code VantagepointCode→live VP tax code + (RateName,ComponentName)→live ACTIVE Xero component. DAG fetches 6 sources (Xero contacts/accounts/tax-rates + VP firms/accounts/tax-codes) feeding run_all via rail.result. Kept QBO orchestration (single read-only open + summarize + premapping-empty suppression). Signal: Status='Error' per failing table + RuntimeError hard-fail (Q5/Q-V1; firm return-bug fixed). Self-heal/archived-cleanup intentionally NOT here (live in sync engines per Q5). Shim _validate block uncommented."
      functional_design: "done (per doc 07 checklist; QBO orchestration template)"
      code_generation: "complete (awaiting approval) — compile + import clean; 6 validate unit tests pass (45 total across all engines)"
      build_and_test: "py_compile + import + 45 unit tests pass; full DAG run (6 source fetches + run_all/summarize, state-error marking) pending dev-airflow"
      depends_on: "U2, U3, U4"
    - id: "U6"
      name: "Documentation parity"
      scope: "mapping_sync/doc/{README.md, LOOKUP_TABLE_FLOWS.md, MAP_FIRM_SYNC_FIX_LOG.md, MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md, MAP_TAX_CODE_SYNC_FIX_LOG.md}. Mirrors QBO doc structure (00 §7). README = package index/convention; LOOKUP_TABLE_FLOWS = state machine + seeded map_account_type + 5 sibling collections; fix-logs record the Q9 Workato-bug fixes per engine (Symptom→Root cause→Fix→Workato ref→Code touchpoints)."
      functional_design: "skip"
      code_generation: "complete (awaiting approval) — 5 docs authored; doc-lint OK (files present, referenced code paths resolve, fix-log structure consistent)"
      build_and_test: "doc lint pass"
      depends_on: "U1"
  extra_tables_added:
    note: "Per user request added 5 collections to common/tables.py + dispatcher init_mapping_collections (NOT mapping_sync steps): map_employee (UNIQUE ContactID), map_bank_code (UNIQUE XeroID), map_currency_code (UNIQUE XeroCode), outstanding_employee_expenses (no key), outstanding_purchase_invoices (no key). Schemas from authoritative 014_501 lookup JSONs."
  build_and_test: "ALL UNITS code-complete; 45 unit tests pass + full-package compileall + import clean + doc-lint OK. PENDING: dev-airflow DAG parse + end-to-end run (rail edit needs restart); then commit. UNCOMMITTED."
```

## Workspace Detection Summary

- **Type:** Brownfield (substantial existing codebase)
- **Primary languages:** Python (dominant), C#, JavaScript/HTML/CSS
- **Build/manifest indicators:**
  - `airflow-integrations/requirements*.txt` (Airflow DAG integrations)
  - `airflow_mapping_framework/config/requirements.txt`
  - `IntegrationPlatform/ClientApp/package.json` (web front-end)
  - `replicon-airflow-library/rail/setup.py`, `replicon-airflow-library/replicon_airflow_provider/setup.py`
- **Sub-projects identified:**
  - `IntegrationPlatform/` — C# backend + ClientApp web UI
  - `airflow-integrations/` — Airflow-based integration DAGs
  - `airflow_mapping_framework/` — mapping framework for Airflow
  - `integration_vantagepoint_quickbooks/` — VantagePoint ↔ QuickBooks integration
  - `integration_vantagepoint_xero/` — VantagePoint ↔ Xero integration
  - `integration_talent_vantagepoint/` — Talent ↔ VantagePoint integration
  - `replicon-airflow-library/` — shared Replicon/Airflow provider library
  - `mappingTables/` — mapping table data
  - `docs-vantagepoint-quickbooks/` — documentation
- **Existing reverse-engineering artifacts:** None found

> NOTE: The detailed `deltek-aidlc/rules/` rule files were not present in the skill install; embedded rules from SKILL.md are being applied.
