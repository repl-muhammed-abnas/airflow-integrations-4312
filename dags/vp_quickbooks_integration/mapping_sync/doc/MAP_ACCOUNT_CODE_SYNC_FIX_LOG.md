# `map_account_code` sync — fix log

A running log of the bugs hit in `process_qbo_accounts` (the
python_callable that powers `sync_qbo_accounts_to_vp` in
`utils/python_callable_method.py`) during trial runs against the
deployed Vantagepoint tenant, and what the fix ended up being.
Siblings: `MAP_FIRM_SYNC_FIX_LOG.md`, `MAP_EMPLOYEE_SYNC_FIX_LOG.md`,
`MAP_TAX_CODE_SYNC_FIX_LOG.md`.

Each entry is structured:

- **Symptom** — the exact error message or behavior observed
- **Root cause** — why it happened
- **Fix** — what changed in code
- **Workato reference** — the canonical Workato recipe / vendor_sync
  helper that informed the fix
- **Code touchpoints** — where the fix lives now

---

## 1. POST `/vision/Accounts/` rejected with `Field Description does not exist` + strict Workato body parity

- **Symptom**: `process_qbo_accounts` POST `/vision/Accounts/` failed
  for every QBO account that resolved through `account_type_map`
  (46 of 92 records) with `Failed with error: Field Description does
  not exist. Field Description does not exist.` The POST body
  included:
  ```
  {"Account": "Accounting", "Name": "Accounting", "Type": "5",
   "Status": "A", "Description": "Accounting", "QBOAccountID": "69"}
  ```
  The remaining 46 records were correctly skipped — their QBO `Type`
  values (Accounts Payable, Accounts Receivable, Income, Cost of
  Goods Sold, Bank, Fixed Asset, Other Expense, Other Current Asset,
  Other Income, Credit Card, Long Term Liability, Other Current
  Liability) don't have entries in the static `account_type_map`
  seed (Workato lookup table only maps 5 QBO types — Asset, Equity,
  Expense, Liability, Revenue — so the skips match recipe parity).
- **Root cause**: `build_vp_account_create_body_from_qbo` and the
  update builder sent fields that don't exist on VP's
  `/vision/Accounts/` schema. Specifically `Description` — VP
  rejects unknown fields with `Field <name> does not exist`. The
  Workato GL recipe `014_503_psa_sync_account_codes.recipe.json`
  POST body (lines 2818-2830) sends a different shape entirely:
  ```
  Status: "A"
  Type:   <MappedVantagepointTypeCode || QBOType fallback>
  Account: <QBOCode>
  Name:    <QBOName>
  CashBasisAccount:        =blank
  UnrealizedLossAccount:   =blank
  UnrealizedGainAccount:   =blank
  CashBasisRevaluation:    =blank
  QBOAccountID:            =blank      (Workato sends blank — see
                                        deferred alt 1b)
  Detail:  "1"
  ```
  No `Description` field anywhere. `Detail: "1"` is a hardcoded
  required-ish field (detail vs summary account distinction at the
  VP side). The four `=blank` fields are explicit-clear semantics
  for PUT (so an existing VP record's CashBasisAccount etc. don't
  leak through) but harmless to omit on POST when there's no
  existing value to clear.
- **Fix**: rewrote both body builders to match Workato's field set
  minus `Description`. The new bodies send:
  - `Account` (POST only — PUT puts it in the URL)
  - `Name`, `Type`, `Status: 'A'`, `Detail: '1'`
  - `QBOAccountID` populated with the actual QBO Id (NOT blank —
    deviation from strict recipe parity; see deferred alt 1b
    below). The map_account_code lookup is the authoritative
    cross-reference, but populating QBOAccountID at the VP side
    makes cross-system debugging (looking at a VP account record
    and tracing back to QBO) immediate.
- **Workato reference**:
  - `014_503_psa_sync_account_codes.recipe.json`:
    - POST body lines 2818-2830
    - PUT body lines 3463-3475 (same shape; `Account` references
      the existing `VantagepointCode` instead of `QBOCode`)
  - `014_503_psa_account_type_map.lookup_table.json` — the static
    5-row CSV seed at line 76 confirms only Asset/Equity/Expense/
    Liability/Revenue are mapped. The "skipped: no entry for QBO
    type" warnings match recipe behavior.
- **Code touchpoints**:
  - `utils/python_callable_method.py:build_vp_account_create_body_from_qbo`
    (rewritten)
  - `utils/python_callable_method.py:build_vp_account_update_body_from_qbo`
    (rewritten)

### Future-revisit alternatives (intentionally not applied)

- **1a. Restore `Description` if VP ever adds the field**: QBO
  accounts carry a `Description` text that's currently lost in the
  sync. Workato also drops it. If VP's Accounts schema ever adds a
  Description column and `Description` becomes a valid field, the
  builder can put it back; until then, sending it errors the POST.
- **1b. Strict recipe parity for `QBOAccountID`** — send `blank`
  (i.e. omit the key) the way the recipe does. The recipe relies
  entirely on the Workato `map_account_code` lookup for the QBO
  cross-reference and doesn't store the ID on the VP side. Our
  deviation (sending the actual ID) is non-breaking — `QBOAccountID`
  is a valid VP field; VP accepts it; subsequent recipe runs would
  still ignore VP's stored value and use the lookup. The benefit is
  debugging: a VP account record opened in the UI shows the QBO
  source ID without needing to consult the map table.
- **1c. Send the `=blank` clearing fields** (CashBasisAccount,
  UnrealizedLossAccount, UnrealizedGainAccount, CashBasisRevaluation)
  on PUT. Workato sends them to explicitly clear any prior values
  on updates. Without them, a PUT preserves whatever the record had
  before. Worth adding if VP-side stale values on these fields
  ever cause a problem; until then, defer.
- **1d. Extend `account_type_map` seed beyond the 5 Workato rows**.
  The 46 skipped QBO accounts all have valid QBO types
  (Income, Bank, etc.) that simply aren't in the seed. Adding rows
  for them would let those QBO accounts sync to VP too. But it's a
  business-policy call — those VP categories don't have unambiguous
  one-to-one mappings, which is presumably why Workato's seed
  leaves them out. Tenant-specific overrides belong in the
  per-customer `account_type_map` post-seed, not in the shared
  default seed.

---

## 2. POST `/vision/Accounts/` rejected with `Column:CashBasisAccount does not exist`

- **Symptom**: after fix #1 dropped `Description` and added `Detail`,
  the next attempt POSTed:
  ```
  {"Account": "Accounting", "Name": "Accounting", "Type": "5",
   "Status": "A", "Detail": "1", "QBOAccountID": "69"}
  ```
  VP rejected every record with `Failed with error:
  Column:CashBasisAccount does not exist. Column:CashBasisAccount
  does not exist.` Different error class from the `Field <name>
  does not exist` we hit in fix #1: VP enforces column **presence**
  at the schema layer for `/Accounts/` and rejects with `Column:`
  prefix when a required key is missing. The four `=blank` fields
  I dropped under deferred-alt #1c (CashBasisAccount,
  UnrealizedLossAccount, UnrealizedGainAccount, CashBasisRevaluation)
  turn out to be column-presence-required, not just default-on-omit.
- **Root cause**: misread of Workato's `=blank` semantics. `=blank`
  in Workato evaluates to an empty value, but the key remains in
  the JSON body the connector serializes — i.e. `{"CashBasisAccount":
  ""}` not omission. VP's POST validation iterates the recipe's
  full output schema and fails the request when any column from
  that schema is absent from the body.
- **Fix**: send all four fields as `''` in both create and update
  body builders. Body is now strict Workato parity (line-for-line
  match with recipe lines 2818-2830 / 3463-3475) except the
  deliberate `QBOAccountID` deviation documented in fix #1 alt 1b.
- **Workato reference**:
  - `014_503_psa_sync_account_codes.recipe.json` POST lines
    2824-2827, PUT lines 3469-3472 — the four `=blank`
    expressions for CashBasisAccount, UnrealizedLossAccount,
    UnrealizedGainAccount, CashBasisRevaluation.
- **Lesson for future field-pruning**: VP error classes are
  meaningful — `Field X does not exist` means "unknown field, drop
  it", `Column:X does not exist` means "schema requires this key,
  send it (even if blank)". Don't conflate the two when trimming
  bodies to match recipe parity.
- **Code touchpoints**:
  - `utils/python_callable_method.py:build_vp_account_create_body_from_qbo`
    (four fields restored)
  - `utils/python_callable_method.py:build_vp_account_update_body_from_qbo`
    (four fields restored)

### Future-revisit alternatives (intentionally not applied)

- **2a. Send `None` instead of `''` and let `_filter_none` drop the
  keys**: would re-introduce fix #2 (`Column:X does not exist`).
  Don't do this.
- **2b. Replace `''` with VP's tenant-default account references**
  on PUT (e.g. lookup the tenant's actual default cash-basis
  account and send its code). Avoids "clearing" semantics for
  existing accounts that have non-default values set in VP. The
  recipe doesn't do this — it sends blank, accepting the clear-on-
  PUT behavior. If a tenant ever complains that their custom
  cash-basis settings are getting wiped on sync, switch to this.

---

## 3. POST `/vision/Accounts/` rejected with `String or binary data would be truncated ... column 'Account'`

- **Symptom**: after fixes #1 and #2 produced a parity-correct body,
  22 PUT updates succeeded (existing records mapped via VP's
  known-good 13-char Account codes), but 24 POSTs failed with VP-
  side database errors like:
  ```
  String or binary data would be truncated in table
  'PDMDemo_IPAAS_REPLICON.dbo.CA', column 'Account'.
  Truncated value: 'Building Repa'.
  The statement has been terminated.
  ```
  The truncated values shown all stop at 13 characters
  (`'Building Repa'`, `'Stationery & '`, `'Maintenance a'`,
  `'Uncategorized'`, etc.). Reading them as exemplars of VP's CA
  table's `Account` column max length: it's 13 characters.
- **Root cause**: VP's `CA.Account` column is a tenant-level
  schema constraint (varchar(13) by inspection of the truncation
  pattern). When the QBO tenant doesn't populate the `AcctNum`
  field, `_qbo_account_code` falls back to QBO `Name` — and many
  QBO account names exceed 13 chars (e.g. 'Building Repairs' = 16,
  'Commissions & fees' = 18, 'Legal & Professional Fees' = 26).
  VP rejects every such POST at the DB layer.
- **Why Workato presumably doesn't hit this**: production tenants
  configure QBO `AcctNum` with short numeric codes (e.g. '5000',
  '6100'), so the fallback to Name is rarely exercised. Our trial
  QBO data has no AcctNum on any account record, exposing the
  fallback path's brittleness against VP's column-width
  constraint.
- **Fix**: gate the POST branch with a length-check, mirroring the
  map_employee fix #2 pattern. When `qbo_code` (the derived VP
  Account value) exceeds 13 chars, log a `WARNING`, increment a
  new `summary['skipped_account_code_too_long']` counter, and
  `continue` to the next record. PUT path is unaffected — its VP
  account code comes from `existing['VantagepointCode']` which is
  already known to be <=13 chars (it was successfully POSTed
  previously, so VP accepted it).
- **Workato reference**: n/a — the Workato recipe doesn't guard
  against this either (recipe line 2822 passes QBOCode directly).
  This is a defensive divergence: Workato fails loudly in this
  scenario; we fail soft with a warning so downstream tasks can
  proceed.
- **Behaviour note**: skipped accounts get NO `map_account_code`
  row. Downstream consumers (validate_mappings, the per-account
  transaction recipes) will see no cross-reference for these QBO
  IDs and behave the same as if QBO had never sent them. The
  warning log is the audit trail.
- **Code touchpoints**:
  - `utils/python_callable_method.py:sync_qbo_accounts_to_vp` —
    summary gains `skipped_account_code_too_long`; POST branch
    gains a length-check before `build_vp_account_create_body_from_qbo`.

### Future-revisit alternatives (intentionally not applied)

- **3a. Truncate `qbo_code` to 13 chars** instead of skipping.
  Preserves the record but risks collisions (e.g. 'Maintenance
  and Repair' and 'Maintenance and Repairs' both truncate to
  'Maintenance a' — second POST fails on the resulting UNIQUE
  conflict). Would need a uniqueness-fixup pass (suffix counter,
  hash) which adds complexity without a clear win over skipping.
- **3b. Deterministic short-code generator** (e.g. acronym +
  hash). Like map_tax_code fix #3 but for accounts. Loses human
  readability of the VP Account code — operators inspecting VP's
  Chart of Accounts UI see opaque codes rather than meaningful
  names. Tenant-side AcctNum configuration is the better answer
  if syncing every QBO account matters.
- **3c. Configuration: prompt the tenant to populate QBO
  AcctNum**. Out-of-band action; not code. The right long-term
  answer for this tenant — `_qbo_account_code` already prefers
  `AcctNum` when present, so configuring it fixes the issue
  without code changes.

---

## 4. POST `/vision/Accounts/` rejected with `Record <Name> already exists and cannot be added`

- **Symptom**: after fixes #1-#3 produced a parity-correct body
  with length-guarded POST, a re-run against a tenant that had
  previously synced (manually or via an earlier trial pass) failed
  for 22 records with `Failed with error: Record <Name> already
  exists and cannot be added.` Examples: `Record Accounting
  already exists`, `Record Advertising already exists`, …
  `Record Travel Meals already exists`. The errored records map
  exactly to the 22 accounts that POSTed successfully on the
  previous trial run.
- **Root cause**: identical to map_employee fix #7. `map_account_code`
  starts empty on a fresh-S3-collection run, so
  `_load_existing_map_account_index` returns `{}` and the per-record
  branch defaults to POST for every QBO record. The VP tenant
  already has those accounts (left over from the earlier successful
  POSTs whose map rows didn't survive the S3 reset), so VP rejects
  with the duplicate-key error.
- **Fix**: per-record VP lookup before deciding POST vs PUT,
  mirroring `_find_vp_firm_by_qbo_id` and
  `_find_vp_employee_by_qbo_id`. New helper
  `_find_vp_account_by_qbo_id(qbo_id, vp_conn_id, context)` issues
  `GET /api/Accounts/?filterHash[0][name]=QBOAccountID&
  filterHash[0][value]=<id>` and returns the VP account dict on a
  match. In `sync_qbo_accounts_to_vp`, when the local `existing_map`
  has no row for the QBOID, the helper is consulted; on a VP hit,
  `existing` is synthesized with `VantagepointCode` = VP's `Account`
  field so the existing PUT branch runs, and
  `_upsert_map_account_code_row` backfills the local map at the end
  of the loop. A new `summary['backfilled_from_vp']` counter tracks
  records that took this path.
- **Why this works for accounts** (vs. tax codes which deliberately
  chose PUT-fallback in fix tax_code #4): VP's `/Accounts/` resource
  carries `QBOAccountID` as a first-class field (we populate it on
  write per fix #1 alt 1b), so the GET-by-QBOID lookup unambiguously
  identifies the right VP record. TaxCodeEntity has no such field
  for the (CodeID, RateID) composite, which is why the tax code
  path uses lazy PUT-fallback instead.
- **Workato reference**: n/a — the Workato sync_accounts recipe
  sends `QBOAccountID: blank` (recipe line 2828) and relies entirely
  on the local lookup table, so it never queries VP by QBOAccountID.
  Our deliberate divergence (sending the actual QBO Id, fix #1
  alt 1b) is what makes this VP-side recovery path possible.
- **Code touchpoints**:
  - `utils/python_callable_method.py:_find_vp_account_by_qbo_id`
    (new helper, near `_load_existing_map_account_index`)
  - `utils/python_callable_method.py:sync_qbo_accounts_to_vp` —
    summary gains `backfilled_from_vp`; per-account loop consults
    the helper when the local map lookup misses; PUT branch runs
    with the synthesized `existing` dict.

### Future-revisit alternatives (intentionally not applied)

- **4a. Catch the "already exists" error and follow up with the
  lookup.** Let the POST go first, parse the error string, then
  do the GET. Saves one VP call per record on the steady-state
  path. Brittle (relies on exact VP error wording) and only wins
  when no QBOAccountID match exists; for most records the upfront
  GET is the right cost.
- **4b. Bulk pre-population via a single `GET /Accounts/?
  filterHash[0][name]=QBOAccountID&filterHash[0][operator]=IsNotBlank`
  at task start.** Loads every VP account that has a QBOID into
  one in-memory dict, drops the per-record GETs. Worth doing if
  account counts get large enough that 92 per-record GETs become
  a measurable cost. Tenant Cust-0012 has ~50 syncable accounts
  so the upfront cost isn't worth the extra code today.

---

## 5. `account_type_map` moved from a seeded S3 collection to a Python constant

- **Symptom**: not an error — a maintainability/consistency cleanup. The
  QBO-type → VP-type lookup was a seeded S3 collection table
  (`account_type_map`, 11 rows) created and populated by
  `dispatcher_dag.init_mapping_collections`, even though it is static,
  read-only product config that no recipe or DAG ever writes.
- **Root cause**: it was modelled as a collection for Workato parity, but
  the sibling static lookups `pay_terms` / `invoice_section_code` had
  already been converted to plain Python constants (`PAY_TERMS_MAP` /
  `INVOICE_SECTION_CODE_MAP`) precisely because they are static and
  universal across tenants. `account_type_map` met the same criteria
  (QBO's `Classification` enum → VP's fixed numeric type codes are
  product-level constants) — `doc/STATIC_CONFIG_LOOKUPS.md` already
  flagged it as a candidate.
- **Fix**: ship it as the `ACCOUNT_TYPE_MAP` constant in
  `common/tables.py` (the 5 QBO-matchable rows; the Workato seed's 6
  empty-`QBOType` rows are VP-only types that never match and were
  dropped). Removed the dead `ACCOUNT_TYPE_MAP_TABLE_NAME / _COLUMNS /
  _SEED_COLUMNS / _SEED_ROWS` constants, the dispatcher table entry, and
  the unused `_shared.py` import. **No behavioral change** — same 5
  mappings; the previously-"unmapped" QBO types stay unmapped. Existing
  tenants keep a now-orphaned `account_type_map` table in their S3
  collection (harmless; nothing reads or creates it). No migration needed.
- **Workato reference**: `014_503_psa_account_type_map.lookup_table.json`
  (the 5-row CSV seed). The recipe's compile join keys this lookup on QBO
  **`Classification`** (`UPPER(qa.Classification) = UPPER(atm.QBOType)`),
  which is what `_resolve_account_type_code` now does — see fix #6.
- **Code touchpoints**:
  - `common/tables.py:ACCOUNT_TYPE_MAP` (new; old table constants removed)
  - `utils/_account_sync.py` — imports `ACCOUNT_TYPE_MAP`
  - `dispatcher_dag.py` — `account_type_map` table no longer created/seeded
  - `utils/_shared.py` — unused import removed

---

## 6. Map data didn't match Workato — rewrite to match existing VP accounts by name (operator-based)

- **Symptom**: the populated `map_account_code` rows diverged sharply from
  the Workato `lookup_table_data_014-503-psa-map-account-code.csv`:
  - Workato's `VantagepointCode` is the **numeric code of an existing VP
    account matched by name** (Accounting → `716.00`, Advertising →
    **`400` AND `6000`**, Cost of Goods Sold → `310` AND `5000`), with
    `VantagepointTypeRO` = that VP account's real type (`9`, `7`, `1`, …).
  - Airflow left `VantagepointCode` empty (or filled it with the QBO Name
    via a POST), stored the `account_type_map` code as `VantagepointTypeRO`
    (`5` for every Expense account), emitted only one row per QBO account,
    and POSTed the Name as the Account code — which produced the
    `Record Fuel/Lawyer/Travel already exists` failures (fix #4 era) and
    the 24 "name > 13 chars" skips (fix #3).
- **Root cause**: the airflow sync never implemented the recipe's core
  step-17 compile join. `014_503_psa_sync_account_codes` matches QBO
  accounts to **existing VP accounts** with
  `LEFT JOIN VantagepointAccounts va ON qa.AcctNum = va.Account OR qa.Name = va.Name`
  and, when the names are equal, records `va.Account / va.Name / va.Type`
  into the map (#23→#25). Several VP accounts sharing a name fan out into
  several map rows. A VP account is **created only when the QBO account has
  an `AcctNum`** and there's no VP match (#26→#27, `QBOCode present`);
  name-only accounts get a QBO-only row with empty VP code. Airflow instead
  resolved a type via `account_type_map` and POSTed the Name as the code —
  inverting the create policy and never reading existing VP accounts by
  name. It also keyed the type-map on `AccountType` rather than
  `Classification`, causing the 48 "no account_type_map entry" warnings.
- **Fix**: re-implemented the sync as an operator-driven port of the
  recipe (mirroring `map_tax_code` / the abbviemst `time_export_child`
  pattern), using the run-local collection operators:
  - DAG stages three collections — `qbo_accounts` (#11), `vp_accounts`
    from a new `fetch_vp_accounts` VP GET (#3/#6/#16), and
    `account_code_map` copied from the S3 map (#13) — then runs the
    step-17 compile JOIN via `QueryCollectionOperator` into
    `compiled_account_codes` (#17).
  - `sync_qbo_accounts_to_vp` is now the step-18 foreach over the compiled
    rows: **match existing VP by name → record `va.Account/Name/Type`**
    (multi-row); **create only when `AcctNum` is present and there's no VP
    match**; PUT on name-drift (#37); otherwise a QBO-only row. The QBO
    `Classification` → VP type code uses `_resolve_account_type_code`
    (the `ACCOUNT_TYPE_MAP` constant, `IFNULL(..., '1')`).
  - `VantagepointTypeRO` is now the matched VP account's `Type` (read-only,
    from VP) instead of the type-map code.
  - The map write stays keyed on `(QBOID, VantagepointCode)` so re-runs
    converge and the multi-row (Advertising → 400 & 6000) shape is kept.
  - This **supersedes the interim "already exists" adoption logic** (the
    by-`QBOAccountID`/by-`Account`-code index + POST-then-adopt path):
    since we no longer POST the Name as the code, the duplicate-key error
    can't occur for name-only accounts, and existing VP accounts are picked
    up by the `va` name join instead.
- **Workato reference**:
  - `014_503_psa_sync_account_codes.recipe.json` — compile query #17 (`va`
    / `acm` / `atm` joins), foreach #18 (add_entry #20, match-record
    #23→#25, create-on-AcctNum #26→#33, name-drift PUT #37→#39).
  - `014_503_psa_synch_accounts.recipe.json` — the Initial-Synch
    orchestrator that starts the Sync Account Codes recipe (#9).
- **Behaviour note / tenant caveat**: this makes the *logic* match Workato;
  exact `VantagepointCode` values depend on the VP tenant's actual
  accounts. The `pentestcustomer` (vantagepointqe2) tenant is polluted with
  name-coded accounts (`Fuel`, `Lawyer`, `Travel`, …) created by the old
  POST-the-Name path — clean those out and reset the `map_account_code`
  collection before re-testing, or the name join will match the bogus
  name-coded accounts instead of the real numeric ones.
- **Code touchpoints**:
  - `utils/_account_sync.py` — staging sources (`build_qbo_accounts_staging`,
    `prepare_vp_accounts_staging`, `read_account_code_map_for_staging`),
    `COMPILE_ACCOUNT_CODES_SQL`, `_read_compiled_account_codes`,
    `_resolve_account_type_code`, rewritten `sync_qbo_accounts_to_vp`;
    removed `_load_vp_accounts_by_qboid` / `_load_existing_map_account_index`.
  - `map_account_code_dag.py` — new `fetch_vp_accounts` +
    `create_qbo_accounts` / `create_vp_accounts` / `create_account_code_map`
    + `query_compiled_account_codes` tasks wired into the populate path.
  - `utils/python_callable_method.py` — re-exports the new staging callables
    and `COMPILE_ACCOUNT_CODES_SQL`.

### Future-revisit alternatives (intentionally not applied)

- **6a. Fetch VP `AccountLength` from `system_formats`** (recipe #29)
  instead of the hardcoded `_VP_ACCOUNT_CODE_MAX_LEN = 13`. **Applied —
  see #7.**
- **6b. `GROUP BY` the compile to one row per QBO account.** The `va` join
  intentionally fans out (multi-row) to match Workato. If a future
  consumer needs exactly one VP code per QBO account, collapse with a
  deterministic pick (as `map_tax_code` does) — but that diverges from the
  Workato data shape this fix targets.

---

## 7. Account-code length guard reads VP System Formats (was hardcoded 13)

- **Symptom**: not a failure — a fidelity/robustness improvement over the
  fix #3 / #6 guard, which hardcoded VP's `CA.Account` width as 13. A
  tenant configured with a different account-number length would either
  reject valid AcctNum-based creates (limit < 13) or hit the DB-side
  truncation error fix #3 was meant to prevent (limit > 13).
- **Root cause**: the 13 was inferred from the observed truncation pattern
  on one tenant, not read from the tenant's actual configuration. The
  Workato recipe reads it at runtime from VP System Formats
  (`014_503_psa_sync_account_codes` #29 → #30 compares
  `QBOCode.length > formats.first.AccountLength`).
- **Fix**: added a `get_system_formats` task
  (`rail.VantagepointSystemFormatsOperator`, GET
  `/api/KeyCvt/CFGFormat/?entity=account`) to `map_account_code_dag`, run
  once per run before the foreach. In
  `_account_sync.py`, `_resolve_vp_account_max_len(context)` reads that
  task's result and probes the response for the account-number length
  (`AccountLength` + version-spelling aliases, mirroring
  `chart_of_accounts_sync._MAX_LENGTH_CANDIDATE_KEYS`), falling back to the
  observed `_VP_ACCOUNT_CODE_DEFAULT_MAX_LEN = 13` when the format can't be
  determined. The foreach resolves the limit once and uses it in the
  create-time guard instead of the constant.
- **Workato reference**:
  - `014_503_psa_sync_account_codes.recipe.json` #29 (`system_formats`
    GET, empty input) and #30 (`QBOCode.length > formats.first.AccountLength`).
  - `chart_of_accounts_sync/processor_dag.py:get_system_formats` and
    `utils/python_callable_method.py:_extract_max_account_length` — the
    established VP System Formats pattern this mirrors.
- **Behaviour note**: degrades gracefully — an unexpected System Formats
  shape or a missing task falls back to 13 rather than blocking creates,
  matching the chart_of_accounts_sync helper's "skip the guard when
  undeterminable" stance.
- **Code touchpoints**:
  - `map_account_code_dag.py` — new `get_system_formats` task wired before
    `process_qbo_accounts`.
  - `utils/_account_sync.py` — `_resolve_vp_account_max_len`,
    `_MAX_ACCOUNT_LENGTH_CANDIDATE_KEYS`, `_VP_ACCOUNT_CODE_DEFAULT_MAX_LEN`
    (renamed from `_VP_ACCOUNT_CODE_MAX_LEN`); foreach resolves the limit
    once per run.

### Future-revisit alternatives (intentionally not applied)

- **7a. Per-tenant override Variable** (like
  `chart_of_accounts_sync._MAX_ACCOUNT_LENGTH_VARIABLE_KEY`). Lets an
  operator force a length without relying on the System Formats probe.
  Add if a tenant's CFGFormat response doesn't expose the length under any
  of the probed keys and the default 13 is wrong for them.
- **7b. Filter the System Formats GET to the account entity** — **Applied.**
  The `get_system_formats` task passes `filters='?entity=account'` (matching
  `chart_of_accounts_sync`), so the length probe reads the account-number
  format directly rather than scanning an unfiltered response. (The recipe's
  call is unfiltered and relies on `formats.first`; narrowing is strictly
  safer.)

---

## 8. `QBOCode` column written blank instead of the QBO `AcctNum`

- **Symptom**: after fix #6, the populated map matched Workato except the
  `QBOCode` column was blank for every row. Workato populates it with the
  QBO `AcctNum` — in the trial tenant only Test_Account (`12345`) and
  Test_Account_01 (`765.99`) have one; all other rows are correctly blank
  in both. Airflow wrote blank even for those two.
- **Root cause**: the fix #6 foreach hardcoded the QBOCode column to `''`
  with a "Workato parity" comment — a wrong assumption based on the
  Workato CSV's column being *mostly* empty (because most accounts lack an
  `AcctNum`). The recipe's compile aliases `qa.AcctNum [QBOCode]` and the
  `add_entry` step #20 writes `col1: QBOCode` (= `qa.AcctNum`); the later
  `update_entry` steps (#25/#36/#42) never touch col1. So col1 is the QBO
  AcctNum for every row. The compiled row already carried it
  (`row['QBOCode']` → `qbo_code`); it was just discarded at write time.
- **Fix**: pass `qbo_code` (the AcctNum) as the QBOCode argument to
  `_upsert_map_account_code_row` instead of `''`. Only the two AcctNum
  accounts change; all others stay blank, matching Workato. The upsert's
  UPDATE branch already updates `QBOCode`, so re-runs stay consistent.
- **Workato reference**: `014_503_psa_sync_account_codes.recipe.json` —
  compile #17 (`qa.AcctNum [QBOCode]`), add_entry #20 (`col1: QBOCode`).
- **Code touchpoints**:
  - `utils/_account_sync.py:sync_qbo_accounts_to_vp` — the
    `_upsert_map_account_code_row(...)` call now passes `qbo_code` for col1.

---

## General notes

- VP's `/vision/Accounts/` endpoint silently ignores some unknown
  fields and loudly rejects others (`Description` is in the loud
  category). The safest body shape is the strict recipe parity
  set; extra fields that aren't in the recipe should be added only
  after confirming the VP API accepts them in the deployed tenant.
- `account_type_map` is intentionally a small fixed lookup: it
  encodes the cross-system convention for how QBO account types
  map into Vantagepoint's `Accounts.Type` numeric code. Adding new
  mappings is a deployment-time decision, not a per-run one.
