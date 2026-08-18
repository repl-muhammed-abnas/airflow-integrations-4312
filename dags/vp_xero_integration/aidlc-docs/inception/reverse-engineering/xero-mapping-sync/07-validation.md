# 07 — Mapping Validation (`Mapping/Validation/`)

**Workato recipes:** `…/014-501 PSA/Mapping/Validation/`
- `014_501_psa_validate_firm_map.recipe.json` → validates `014-501 PSA Map Firm`
- `014_501_psa_validate_account_map.recipe.json` → validates `014-501 PSA Map Chart of Accounts`
- `014_501_psa_validate_tax_map.recipe.json` → validates `014-501 PSA Map Tax Code`

**Airflow target:** `vp_xero_integration/mapping_sync/validate_mappings_dag.py` + `utils/_validate.py` (mirror QBO).

> **Common skeleton (all three):** callable recipe-function, no params, `concurrency 1`. Declare scalar `Message` + a `Messages` list (`{error}`). Pull source systems + the lookup table → build `workato_smart_list` collections → run **referential anti-join checks** (`LEFT JOIN … IS NULL` / `NOT EXISTS`) → accumulate human-readable error strings → `return_result` **`Message = Message.present? ? Messages.to_json : blank`** (empty/blank = pass; non-empty = JSON array of errors).
> **None writes `mapping_table_state`. None halts/raises** — all failures are non-fatal (collected, sometimes logged). These are referential-integrity / self-heal checks on *existing* rows; they do **not** check coverage (unmapped source records) or duplicates.

---

## Comparison matrix

| Aspect | Firm | Account | Tax |
| --- | --- | --- | --- |
| Source pulls | VP `firm` search + Xero `GET /Contacts?includeArchived=true` | Xero `list_accounts` + VP `chart_of_accounts list` (+ Account Type lookup, **unused**) | Xero `list_tax_rates` + VP `tax_codes list` |
| Pre-processing | none | none | `js_eval FlattenTaxRates` (rate→component) |
| Writes to map table | **delete** archived-contact rows only | **update** col6 (VP Type) + col7 (Xero ID) — set or blank | **none** |
| Logs to `014-501 PSA Log message` | **no** | yes (per failure) | yes (per failure) |
| `Message` scalar set on error | **no → BUG: always returns blank** | yes (works) | yes (works) |
| Checks | dangling Xero contact, dangling VP firm | dangling VP account, dangling Xero account | dangling VP code, dangling Xero rate/component |
| Duplicate / coverage / type-validity | none | none (Account-Type map loaded but never queried) | none (`isNewOrUpdated` computed, unused) |

---

## 1. Validate Firm Map (`014-501 PSA Map Firm`)

**Validation rules:**
1. **Archived-contact cleanup** — mapping `col2` (ContactID) joined to a Xero contact with `ContactStatus='ARCHIVED'` → **DELETE the lookup row** (by EntryID). Not reported as an error. *(Only table mutation.)*
2. **Dangling Xero contact** — mapping ContactID not present in pulled Xero contacts (excl. blank ContactID and rows with mapping `Status='ARCHIVED'`) → append `Contact <XeroName> (<ContactID>) not found in Xero`.
3. **Dangling VP firm** — mapping `col1` FirmID not present in VP firms (`ClientID`) (excl. blank, ARCHIVED) → append `Firm <VantagepointName> (<FirmID>) not found in Vantagepoint`.

**Not checked:** required/non-blank flagging, duplicates, coverage, Vendor/Client (col4/col5), Mod Date.
**Calls:** VP `firm` search; Xero `GET /Contacts?includeArchived=true` (needed for the archived check).
⚠ **BUG:** scalar `Message` is never assigned → `Message.present?` is always false → **always returns `blank`** even when errors exist (likely meant `Messages.present?`). It also does **not** call the logging recipe. **Port must fix:** return the `Messages` array when non-empty.

## 2. Validate Account Map (`014-501 PSA Map Chart of Accounts`)

This recipe **self-heals** as it validates (writes corrections by EntryID).

**Validation rules:**
1. **VP referential + col6 sync** — LEFT JOIN map→VP on `Account=col4(VP Code)`. Found → write live VP `Type` into **col6**. Not found → clear col6 (`=blank`), set+log+append `Account <VPName> (<VPCode>) not found in Vantagepoint`. (blank VP code skipped.)
2. **Xero referential + col7 back-fill** — LEFT JOIN map→Xero on `Code=col1(Xero Code)`. (a) found & col7 empty → back-fill **col7** with live AccountID; (b) found & col7 set → no-op; (c) not found → clear col7 (`=blank`), set+log+append `Account <XeroName> (<XeroCode>) not found in Xero`. (blank Xero code skipped.)

**Not checked:** duplicates, non-blank/required, **account-type validity** (the `Map Account Type` table is loaded but **never queried** here), col1–col5 never modified.
**Calls:** Xero `list_accounts`; VP `chart_of_accounts list`; async `014-501 PSA Log message` per failure.
**Outcome:** `Message` assigned on error → returns JSON `Messages` array; self-heal writes happen regardless.

## 3. Validate Tax Map (`014-501 PSA Map Tax Code`)

Read-only (no write-back, despite `col7=Messages` existing). Uses `js_eval FlattenTaxRates` so the Xero side is component-level.

**Validation rules:**
1. **VP tax-code referential** — `col3` (VP Code) not among live VP tax codes (`vtc.Code`) → set+log+append `Tax Code <Code> not found in Vantagepoint`. (blank skipped.)
2. **Xero rate/component referential** — `(col1 RateName, col2 ComponentName)` pair not among the flattened **ACTIVE** Xero components → set+log+append `Tax rate/component <Name>/<Code> not found in Xero`. (A mapping to an *inactive* Xero rate is therefore flagged "not found.")
3. **(computed, unused)** `isNewOrUpdated` rate-drift flag — present but no step acts on it (belongs to the sync recipe).

**Not checked:** duplicates, required-field, Sequence/CompoundOnCode, **col7 Messages is read but never written**.
**Calls:** Xero `list_tax_rates`; VP `tax_codes list`; async `014-501 PSA Log message` per failure.
**Outcome:** `Message` assigned on error → returns JSON `Messages` array. Fully idempotent (no writes).

---

## 4. Airflow design notes (validation → `validate_mappings_dag.py` / `_validate.py`)

The QBO reference already has `validate_mappings_dag.py` + `utils/_validate.py` with `run_all_mapping_validations` + `summarize_mapping_validations` and per-table validators, run as **phase 5** (after firm/employee/account/tax), hard-failing the dispatcher on missing-key issues and warning on business-rule misses. Mirror that, folding in these Xero rules:

- **Per-table referential checks** (the anti-joins above): in one S3 open, LEFT JOIN each `map_*` collection against freshly-fetched Xero + VP data; collect errors.
- **Self-heal (accounts):** reproduce col6 (VP Type) refresh + col7 (XeroID) back-fill. Decide whether self-heal stays in validation or moves into the account sync engine (cleaner to keep healing in the sync; keep validation read-only/reporting).
- **Archived cleanup (firm):** reproduce the delete-archived-contact-rows behaviour (or move it into the firm sync). Requires fetching Xero contacts **with archived included**.
- **Signal via `mapping_table_state`:** unlike Workato (which doesn't touch state here), the QBO Airflow port marks `Status='Error'` per failing table and hard-fails the dispatcher. **Adopt the QBO behaviour** (state-driven) rather than the Workato return-string behaviour.
- **Fix the firm return bug** — surface errors when the `Messages` list is non-empty.
- **Decide on coverage/duplicate checks:** Workato validates only existing rows. Consider adding (a) duplicate-key detection (UNIQUE index already enforces this at write time) and (b) optional coverage reporting (active Xero records with no mapping row) as Airflow improvements — flag as story acceptance criteria, not silent additions.
- **Page Xero list calls** (contacts especially) for the referential checks.

### Validation rules — consolidated checklist for `_validate.py`
| Table | Rule | On fail (QBO-style) |
| --- | --- | --- |
| map_firm | ContactID resolves to a live Xero contact | error → state Error |
| map_firm | FirmID resolves to a live VP firm | error → state Error |
| map_firm | archived Xero contact → remove row | cleanup (firm sync) |
| map_chart_of_accounts | VantagepointCode resolves to live VP account | error |
| map_chart_of_accounts | XeroCode/XeroID resolves to live Xero account | error |
| map_chart_of_accounts | refresh VP Type (col6), back-fill XeroID (col7) | self-heal (account sync) |
| map_tax_code | VantagepointCode resolves to live VP tax code | error |
| map_tax_code | (XeroName, XeroCode) resolves to live ACTIVE Xero rate/component | error |
