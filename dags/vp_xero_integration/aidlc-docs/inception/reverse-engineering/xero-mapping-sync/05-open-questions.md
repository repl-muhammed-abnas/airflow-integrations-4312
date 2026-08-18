# 05 — Open Questions & Decisions (resolve in/around the User Story stage)

These must be answered before or during User Story authoring; each affects the Airflow design.

## Cross-cutting / architecture

| ID | Question | Why it matters | Recommendation |
| --- | --- | --- | --- |
| Q-X1 | ~~employee sync in scope?~~ **RESOLVED: No** (Q1=A). Firms/accounts/tax only; US-7 descoped. | — | — |
| Q-X2 | ~~RAIL Xero operators?~~ **RESOLVED** (Q3): RAIL has `xero_internal` (generic `XeroAPIOperator` + `XeroHook`). Full inventory + gaps in [08-xero-api-inventory.md](08-xero-api-inventory.md); enablement tracked as **US-9**. mapping_sync hard-depends only on **G1 pagination**. | — | — |
| Q-X3 | What is the **CFG block** the Xero middleware ships (e.g. `CFG_UpgradeDataSync`, `CFG_Region`, `CFG_DefaultVendorType`…)? | Drives `apply_premapping_state` and defaults. | Match QBO CFG keys unless the Xero middleware differs. |
| Q-X4 | **Connection id + conf key** for Xero. | `xero_default` / `connections.xero` assumed. | Confirm against middleware integration config. |
| Q-X5 | Keep `map_account_type` as a **seeded S3 collection** or a **static Python constant**? | Xero file ships data; QBO used a static constant. | Seeded collection (data-driven, matches Workato). |

## Firms ([01](01-synch-firms.md))

| ID | Question | Risk |
| --- | --- | --- |
| Q-F1 | **Xero pagination**: the recipe does a single unpaged `GET /Contacts`. | Large tenants silently miss contacts. The port **must page** (active + archived). |
| Q-F2 | **Name-only matching** (with `MIN(ClientID)`): acceptable precision? | Mis-map on duplicate names; miss on whitespace/case/punctuation differences. Consider persisting ContactID into a VP custom field for future id-based matching. |
| Q-F3 | ~~**Placeholder-seeding contract**~~ **ANSWERED** ([06](06-lookup-table-seeding.md)): `Mapping/Lookup Tables/014_501_psa_map_firms` seeds blank-FirmID rows (one per active Xero contact, keyed by ContactID), gated by "table empty." Open follow-up: in Airflow, **merge seeding into the `map_firm` engine** (QBO parity) or keep a separate seed task? Recommended: merge. | Decide merge-vs-separate; fix the no-`IsCustomer/IsSupplier`-filter + no-pagination issues in the port. |
| Q-F4 | **AccountNumber convention** `SL<client>/PL<vendor>` — holds for all tenants? | Wrong Vendor/Client codes (col4/col5) otherwise. |
| Q-F5 | **Default Org** = first VP org. Multi-org behaviour? | Workato comment says corrected manually in VP. Define desired behaviour. |
| Q-F6 | **Address dedup**: Workato uses fresh `CLAddressID` uuid each run → possible duplicate addresses on re-create. | Add a dedup key in the port (improvement). |
| Q-F7 | **Country/State resolution by Description** (LEFT JOIN); unmatched → NULL silently. | Decide fallback/logging for unmatched geo. |
| Q-F8 | **Direction**: confirm the VP→Xero create half lives in a sibling recipe (this recipe is Xero→VP only). | Completes the initial-sync picture. |

## Accounts ([02](02-synch-accounts.md))

| ID | Question | Risk |
| --- | --- | --- |
| Q-A1 | `map_account_type` representation (see Q-X5). | — |
| Q-A2 | **Orphan deactivation** (orchestrator step 12 sets `Status=I` for any VP account not in the mapping table) — should it deactivate **manually-created VP accounts** too? | Could wrongly deactivate non-Xero VP accounts. Gate or scope it. |
| Q-A3 | **Account length violation**: Workato logs but leaves the row unmapped → logs every run. | Improve: write `Messages` + mark once, or surface as a validation error. |
| Q-A4 | `XeroName.slice(0,39)` vs VP 40-char field — confirm truncation length. | Off-by-one. |
| Q-A5 | First population of `Map Chart of Accounts`: orchestrator only calls worker for rows already present with a Xero ID but no VP code — how are brand-new Xero accounts first seeded? | Worker with blank `XeroCode` lists all ACTIVE Xero accounts; confirm seeding path. |

## Tax codes ([03](03-sync-tax-codes.md))

| ID | Question | Risk |
| --- | --- | --- |
| Q-T1 | **Step-16 join precedence**: `… ON A AND B OR C` is unparenthesized → C applies broadly. | Possible duplicate/incorrect VP matches. Fix with explicit parentheses in the port; confirm intended semantics. |
| Q-T2 | **VP-code generator** `'X'+Sequence.rjust(4,'0')` caps at 9999. | Acceptable ceiling? |
| Q-T3 | **`ReverseCharge`** is the only VP behavioural flag set; ApplyTo*/accounts/region left default on create. | Confirm VP defaults acceptable for new codes. |
| Q-T4 | **`IsNonRecoverable`** is in Xero raw data but not exposed as a datapill. | If non-recoverable VAT is needed, RAIL Xero op must expose it. |
| Q-T5 | **Compound linking** correctness across >2 components / multiple compound components per rate. | Workato picks `LIMIT 1` non-compound base — verify for complex rates. |
| Q-T6 | **Region gating**: QBO created tax groups only when `CFG_Region=='US'`. Does Xero have an equivalent regional branch? | Region-specific behaviour. |

## Seeding (`Lookup Tables/`) ([06](06-lookup-table-seeding.md))

| ID | Question | Risk |
| --- | --- | --- |
| Q-S1 | ~~Merge seeding or separate?~~ **RESOLVED: Merge** into `map_*` engines (Q2=Yes, QBO parity). | — |
| Q-S2 | Firm seeder writes **all active contacts with blank Vendor/Client** (no `IsCustomer`/`IsSupplier` filter). Should the port derive Vendor/Client at seed time? | Placeholder rows lack ledger classification. |
| Q-S3 | Account seeder **INNER JOINs `Map Account Type`** → Xero accounts with unmapped types are silently dropped. Intended filter or data loss? | Missing accounts. |
| Q-S4 | Tax seeder `add_batch` references `rows.first.*` (probable datapill bug) instead of the current item. | If real, all seeded rows get first-row values — **port must iterate per-row**; verify against live table. |
| Q-S5 | All seeders are **non-paginated** and **gated by "table empty"** (no top-up). | Under-seeding on large tenants; manual clear needed to re-seed. |

## Validation (`Validation/`) ([07](07-validation.md))

| ID | Question | Risk |
| --- | --- | --- |
| Q-V1 | `validate_firm_map` has a **return bug** (`Message` never set → always returns `blank`). Port must surface `Messages` when non-empty. | Errors silently swallowed. |
| Q-V2 | Should **self-heal** (account col6/col7 refresh; firm archived-row delete) live in the **validation** DAG or in the **sync** engines? | Recommended: heal in sync, keep validation read-only/reporting. |
| Q-V3 | Signal failures via **`mapping_table_state` Status='Error'** + dispatcher hard-fail (QBO behaviour) rather than Workato's return-string? | Adopt QBO state-driven behaviour. |
| Q-V4 | Add **duplicate-key** and **coverage** (unmapped active source records) checks as Airflow improvements? | Workato checks neither; surface as story criteria, not silent additions. |
| Q-V5 | Account validation loads `Map Account Type` but **never uses it** — is an account-type-validity check desired? | Possible missing rule. |

## Logging / observability (all)

| ID | Question |
| --- | --- |
| Q-L1 | Workato writes failures to the `014-501 PSA Log` lookup. QBO uses RAIL log operators instead. Confirm Xero uses RAIL logging (recommended) rather than a `log` collection. |
| Q-L2 | Workato emits only "Error" log rows (no positive per-record audit). Is an Airflow run summary/metrics desirable? |
| Q-L3 | The firms "success" path reuses the **error-notification** recipe to send a completion email. Confirm desired notification behaviour in Airflow (middleware `PostDagRunDetails` vs email). |

---

### Suggested resolution path
Most of these are best captured as **acceptance-criteria / assumptions** on the User Stories (one story per child DAG: Firm, Account, Tax, + Dispatcher/Init, + Validation). Q-X1/Q-X2 (scope + RAIL operator availability) and Q-F3 (placeholder seeding) are **blocking** — resolve before development planning.
