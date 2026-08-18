# `map_firm` sync — fix log

A running log of the bugs hit in `process_qbo_firms` (the python_callable
that powers `sync_qbo_firms_to_vp` in
`utils/python_callable_method.py`) during the first round of trial
runs against the deployed Vantagepoint tenant, and what the fix
ended up being. Future me / future Claude / future maintainer: when
something in this sync stops working, check here first — every fix
below was driven by a real VP error message that surfaced in
`vp_qbo_mapping_sync_logs_*.log`.

Each entry is structured:

- **Symptom** — the exact error message or behavior observed
- **Root cause** — why it happened
- **Fix** — what changed in code
- **Workato reference** — the canonical Workato recipe / vendor_sync
  helper that informed the fix (this integration is a 1:1 port of
  the Workato `014-503 PSA` recipes, so when in doubt, mirror
  what they do)
- **Code touchpoints** — where the fix lives now

The order is roughly chronological — fixes earlier in the list
were discovered first; later ones only became visible after the
earlier ones unblocked execution past them.

---

## 1. `intuit_conn_id` resolving to a Vantagepoint connection ID

- **Symptom**:
  `AirflowNotFoundException: The conn_id 'vantagepoint_dev_conn' isn't defined`
  raised by `QuickBooksCustomerOperator` in `map_firm_dag.fetch_qbo_customers`.
- **Root cause**: `mapping_sync` had inverted the project's
  `vantagepoint` / `intuit` convention. The
  rest of the repo (`vendor_sync/*`,
  `vantagepoint-integration-builder-prompt.md:257`) uses
  `intuit` for VP and `vantagepoint` for QBO.
  `mapping_sync/config.py:get_conn_ids` and the four
  `map_*_dag.py` files had them swapped, so the dispatcher's
  payload (`intuit = vantagepoint_dev_conn`) landed
  in the slot the QBO operator was reading.
- **Fix**: aligned `mapping_sync` with the project-wide convention.
  `intuit` → VP, `vantagepoint` → QBO. Updated:
  - `config.py:get_conn_ids` mapping + docstring
  - `map_firm_dag.py`, `map_employee_dag.py`,
    `map_account_code_dag.py`, `map_tax_code_dag.py`: every
    `intuit_conn_id` template now reads `vantagepoint`
- **Workato reference**: not directly — this is a project-internal
  convention. `vendor_sync/dispatcher_dag.py:89` shows the canonical
  shape (`intuit_conn_id={{ ... vantagepoint }}`).
- **Code touchpoints**: 4 child DAGs + `config.py`.

---

## 2. Contact POST body — `Field FirmID does not exist`

- **Symptom**:
  `Failed with error: Field FirmID does not exist. Field FirmID does not exist.`
  on `POST /api/contact` for every customer with a contact name.
- **Root cause**: `build_vp_firm_contact_body_from_qbo` was sending
  Workato/QBO-shaped field names that VP's `/contact` endpoint
  doesn't recognize:
  - `FirmID` instead of `ClientID`
  - `PrimaryEmail` instead of `Email`
  - `Mobile` instead of `CellPhone`
  - `IsPrimary: True` instead of `QBOIsMainContact: 'true'`
  - Missing required `ContactStatus`
  - Missing `QBOID` (so contacts couldn't be traced back to QBO)
- **Fix**: rewrote the body to match VP's actual contact schema,
  mirroring the canonical `vendor_sync.build_create_contact_body`
  (514). Final body keys:
  ```
  ClientID, ContactStatus='A', FirstName, LastName,
  Email, Phone, CellPhone, Fax, QBOID, QBOIsMainContact='true'
  ```
- **Workato reference**:
  `014_503_psa_quickbooks_contact_to_vantagepoint.recipe.json` +
  `vendor_sync.build_create_contact_body`.
- **Code touchpoints**:
  `utils/python_callable_method.py:build_vp_firm_contact_body_from_qbo`.

---

## 3. Address POST body — `Country Code USA does not exist`

- **Symptom**:
  `Failed with error: Country Code USA does not exist.`
  on `POST /api/firm/{ClientID}/address` for one record (Kate
  Whelan / QBOID 14) whose QBO `BillAddr.Country` was the string
  `"USA"`.
- **Root cause**: VP's country master list has `United States` but
  not the alias `USA` / `US` / `U.S.` / etc. The address body
  passed `BillAddr.Country` through verbatim from QBO.
- **Fix**: added `_VP_COUNTRY_ALIASES` dict +
  `_normalize_vp_country()` helper. Applied at the `Country` slot
  in the address body. Maps US / USA / U.S. / U.S.A. / America /
  `United States of America` → `United States`; UK / U.K. / Great
  Britain → `United Kingdom`; CA → Canada. Unknown values pass
  through unchanged so adding new aliases is just a dict edit.
- **Workato reference**: the recipes pass `BillAddr.Country`
  through unchanged too — this is a tenant-side master-data quirk
  rather than a recipe behavior, so the fix is a defense layer
  on top of recipe parity.
- **Code touchpoints**:
  `utils/python_callable_method.py` — module-level
  `_VP_COUNTRY_ALIASES` + `_normalize_vp_country()` +
  `build_vp_firm_address_body_from_qbo` call site.

---

## 4. POST `/firm` colliding with existing VP records

- **Symptom**:
  `Failed with error: Firm Number already exists: C00131.<BR/>Vendor Number already exists: 000056.`
  on every vendor's first `POST /api/firm`. VP auto-assigned the
  next available Firm Number / Vendor Number, which already
  existed in the tenant (seeded from prior Workato runs).
- **Root cause**: `_load_existing_map_firm_index` only checks the
  *local* `map_firm` S3 table, which starts empty on a fresh
  customer. VP-resident firms (created by Workato or imported
  manually) are invisible to that check, so the loop went to the
  POST branch and clobbered. VP's tenant numbering ruleset
  rejected the duplicate.
- **Fix**: new helper `_find_vp_firm_by_qbo_id(qbo_id, is_vendor_flag,
  vp_conn_id, context)` that does
  `GET /api/firm?filterHash[0][name]=QBOID&filterHash[0][value]=<qbo_id>`
  whenever the local `map_firm` index misses. If VP already has a
  firm with that QBOID (and matching VendorInd, for safety against
  rare tenants that have both a customer-firm and a vendor-firm
  with the same QBOID), `_process_one` routes to the PUT update
  path with the existing `ClientID` instead of POSTing a duplicate.
  The successful PUT path backfills `map_firm` via the normal
  `_upsert_map_firm_row` at the end of the per-record loop, so the
  extra GET only fires once per VP-resident firm and never again.
- **Workato reference**: the canonical firm-existence lookup is in
  `vendor_sync` via a per-vendor `firm_map` cache (see
  `vendor_sync.find_firm_in_firm_map`); we don't have that cache
  yet in `mapping_sync`, so the GET-by-QBOID call is the
  thinnest-possible equivalent until `map_firm` is fully populated.
- **Code touchpoints**:
  `utils/python_callable_method.py:_find_vp_firm_by_qbo_id` +
  `sync_qbo_firms_to_vp._process_one` (the "local miss → VP
  lookup" block right before the PUT/POST branch).

---

## 5. Contact POST — `Please provide a Last for table Contacts`

- **Symptom**:
  `Failed with error: Please provide a Last for table Contacts.`
  on `POST /api/contact` for QBOID 9 ("55 Twin Lane") — QBO
  contact had `GivenName="Amelia"` but no `FamilyName`.
- **Root cause**:
  `build_vp_firm_contact_body_from_qbo` skipped the body only
  when *both* `GivenName` and `FamilyName` were absent. A record
  with only `GivenName` sent through with `LastName=''`, and VP
  enforces LastName at the API level.
- **Fix**: changed the early-return guard to skip the contact
  entirely when `FamilyName` is missing. Fabricating a placeholder
  LastName would pollute VP's contact list.
- **Workato reference**: recipe doesn't have an explicit guard
  either — Workato users hit the same error and the support
  pattern is to fix the QBO source data rather than send junk to
  VP. Our defensive skip is a step beyond recipe parity.
- **Code touchpoints**:
  `utils/python_callable_method.py:build_vp_firm_contact_body_from_qbo`
  (the `if not family: return None` guard).

---

## 6. VendorAccountingInfo POST — `Object reference not set to an instance of an object` (NPE), single-field body

- **Symptom**: every vendor (~32) failed
  `POST /vision/firm/VendorAccountingInfo` with VP's C#
  `NullReferenceException`:
  `Failed with error: Object reference not set to an instance of an object.`
- **Root cause**: the body was a one-line `{"ClientID": client_id}`
  stub. VP's endpoint doesn't take `ClientID` at all — it
  dereferences a `Vendor` field (the VP-side vendor code, not the
  firm reference) plus a paired set of defaults the Workato recipe
  always sends. Sending just `ClientID` left `Vendor=null` on the
  deserialized entity and the server-side validator NPE'd.
- **Fix**: rewrote `build_ve_accounting_body` to emit the full
  Workato-canonical body. Field map matches
  `014_503_psa_dvp_insert_update_veaccounting.recipe.json` and
  `vendor_sync.build_create_veaccounting_body`:
  ```
  Vendor:            <VP vendor code — see fix #7>
  PayTerms / PayTermsDesc: from vp_qbo_vendor_sync_pay_terms_map
                            Variable, '' fallback
  Req1099:           'Y' / 'N' from qbo_record.Vendor1099, default 'N'
  DiscPct, DiscPeriod, ThisYear1099, LastYear1099: 0
  CheckPerVoucher, MemoPrintOnCheck, EFTAddenda,
  EFTRemittance, EFTClieOp: 'N'
  ```
  **Deliberately omitted**: `Company` and `ElectronicPaymentMethodID`.
  Workato's `=blank` syntax for those means "don't send"; sending
  them as empty strings raises
  `Cannot insert NULL into Company column` per
  `vendor_sync.build_create_veaccounting_body`'s docstring.
- **Workato reference**: recipe
  `014_503_psa_dvp_insert_update_veaccounting` POST step +
  `vendor_sync.build_create_veaccounting_body`.
- **Code touchpoints**:
  `utils/python_callable_method.py:build_ve_accounting_body`.

---

## 7. VendorAccountingInfo POST — same NPE with full body but raw QBO id for `Vendor`

- **Symptom**: after fix #6 the body was complete but VP still
  NPE'd on every vendor.
- **Root cause**: `Vendor` was being passed as
  `str(qbo_record['Id'])` (e.g. `"56"`). VP's tenant numbering
  rule rewrites the stored vendor code (e.g. `56` → `000056`,
  visible in earlier `Vendor Number already exists: 000056`
  errors). The VendorAccountingInfo endpoint looks up the vendor
  by that stored code; `"56"` doesn't match any vendor; VP
  dereferences the missing relation and NPE's.
- **Fix**: added `_resolve_vp_vendor_code(client_id, vp_conn_id,
  context)` (later renamed / extended — see fix #9). Fires a
  `GET /api/firm/{ClientID}` and reads back the firm's actual
  stored `Vendor` field. `build_ve_accounting_body` now takes the
  resolved code as an explicit param. Call site skips the POST
  with a warning if VP returns no Vendor code (rather than firing
  a body guaranteed to NPE).
- **Workato reference**: `vendor_sync._resolve_vp_vendor_code` —
  documented rationale: "we don't ASSUME the stored value matches".
  Same problem, same fix shape; implementation differs because
  `vendor_sync` reads a DAG-task XCom while we're inside a
  per-record Python loop.
- **Code touchpoints**:
  `utils/python_callable_method.py:_resolve_vp_firm_for_veaccounting`
  (current name; was `_resolve_vp_vendor_code`).

---

## 8. VendorAccountingInfo POST — same NPE again, now with VP-resolved `Vendor: "000056"`

- **Symptom**: after fix #7 the body had `Vendor: "000056"`
  (correctly resolved), but VP **still** NPE'd on every vendor.
- **Root cause**: the Workato recipe
  `014_503_psa_dvp_insert_update_veaccounting` is an
  **upsert**, not a plain insert. Two distinct code paths
  selected by checking whether VendorAccountingInfo already
  exists for the firm:
  ```
              ┌─ existing → PUT /api/firm/{ClientID}
              │             body: {"VEAccounting": [{PayTerms, Req1099}]}
  upsert ────┤
              │
              └─ none → POST /vision/firm/VendorAccountingInfo/
                          body: {Vendor, PayTerms, ...full schema}
  ```
  These vendors already had VendorAccountingInfo records (auto-created
  by VP when the firm was registered with `VendorInd=Y`, or
  pre-seeded from prior Workato runs). Our always-POST behavior
  hit VP's duplicate-insert path → NPE.
- **Fix**: added `build_ve_accounting_update_body(qbo_record,
  existing_ve_accounting)` — emits
  `{"VEAccounting": [{"PayTerms": ..., "Req1099": ...}]}`
  for the PUT update branch. PayTerms resolution chain mirrors
  `vendor_sync.build_update_veaccounting_body`: lookup-map →
  existing-row fallback → `''`. The vendor-side `_process_one`
  block now branches on whether existing rows are present:
  - non-empty → PUT `/api/firm/{ClientID}` (via the same
    `VantagepointFirmOperator` used for firm updates, with the
    VEAccounting-array body)
  - empty + vendor code resolved → POST as before
  - empty + no vendor code → skip with a warning
- **Workato reference**: recipe
  `014_503_psa_dvp_insert_update_veaccounting` (both PUT and POST
  branches) + `vendor_sync.build_update_veaccounting_body` +
  `vendor_sync.vendor_update_dag` upsert flow.
- **Code touchpoints**:
  `utils/python_callable_method.py:build_ve_accounting_update_body`
  +
  `sync_qbo_firms_to_vp._process_one` (vendor branch).

---

## 9. VendorAccountingInfo upsert — branch decision was reading the wrong existence signal

- **Symptom**: after fix #8 the upsert was wired but VP still
  NPE'd for every vendor with the same `Object reference not set
  to an instance of an object` and the failing call was always
  the POST (insert) branch. `existing_ve_accounting` was coming
  back empty even when VP had records.
- **Root cause**: `_resolve_vp_firm_for_veaccounting` was checking
  the `VEAccounting` field on the **firm root** response
  (`GET /api/firm/{ClientID}`). That field is not reliably
  populated on the firm root — VP exposes existing
  VendorAccountingInfo rows via a dedicated sub-resource
  endpoint instead. The recipe (and `vendor_sync.vendor_update_dag`)
  use it too:
  ```
  GET /api/firm/{ClientID}/vendorAccountingInfo
  ```
- **Fix**: the resolver now makes two GETs:
  1. `VantagepointFirmOperator` → `GET /api/firm/{ClientID}` →
     extract `Vendor` code
  2. `VantagepointAPIOperator` →
     `GET /api/firm/{ClientID}/vendorAccountingInfo` → extract
     existing rows
  The second response is normalized (list/dict/None → list of
  non-empty dicts); empty dicts are filtered so they don't
  falsely trigger the update branch. A 404 (some tenants don't
  materialize the sub-resource until first insert) is caught and
  treated as "no rows → insert branch", matching recipe behavior.
- **Workato reference**:
  `014_503_psa_dvp_insert_update_veaccounting` has 4 distinct
  endpoint paths visible via `grep`:
  1. `api/firm/{ClientID}/vendorAccountingInfo` (GET — existence)
  2. same (GET — read existing for PayTerms fallback)
  3. `api/firm/{ClientID}` (PUT — update via VEAccounting array)
  4. `vision/firm/VendorAccountingInfo/` (POST — insert)
  `vendor_sync.vendor_update_dag:278` (`get_firm_veaccounting`
  task) uses the same `VantagepointAPIOperator` against the same
  sub-resource path.
- **Code touchpoints**:
  `utils/python_callable_method.py:_resolve_vp_firm_for_veaccounting`.

---

## 10. `reverse_sync_vp_clients` — removed entirely (no Workato analogue)

- **Symptom**: `reverse_sync_vp_clients` POSTed 129 unmapped VP firms
  to QBO `/customer` and every single one failed with
  `Duplicate Name Exists Error, code 6240`. Most failures were
  standard QBO sandbox sample names (Amy's Bird Sanctuary, Cool
  Cars, Bill's Windsurf Shop, Freeman Sporting Goods) and VP-side
  duplicate-named firms (6 distinct `ClientID`s all named
  "Freeman Sporting Goods").
- **Root cause**: the bulk reverse-sync was a design that had **no
  Workato analogue**. The Workato initial-sync recipe
  `014_503_psa_synch_firms` is forward-only (QBO → VP). The only
  VP → QBO push in Workato is the event-driven polling trigger
  `014_503_psa_customer_upserted_in_vantagepoint` → recipe
  `014_503_psa_vantagepoint_customer_to_quickbooks`, which fires
  per-firm when a VP firm changes (not a bulk scan). The Airflow
  bulk-scan-and-POST shape would always collide with QBO's
  `DisplayName` uniqueness constraint for tenants where QBO and VP
  both pre-existed.
- **Fix**: removed reverse-sync from `mapping_sync` to align with
  the Workato initial-sync shape (forward-only). Deleted:
  - `reverse_sync_unmapped_vp_clients_to_qbo` (the function)
  - `build_qbo_customer_body_from_vp_firm` (its only helper)
  - the `reverse_sync_vp_clients` `PythonOperator` task in
    `map_firm_dag.py` and its graph wires
- **Workato reference**:
  - `014_503_psa_synch_firms.recipe.json` — forward-only initial
    sync; no VP→QBO branch
  - `014_503_psa_customer_upserted_in_vantagepoint.recipe.json` +
    `014_503_psa_vantagepoint_customer_to_quickbooks.recipe.json`
    — the per-firm VP→QBO push, an event-driven recipe that
    belongs in a separate trigger DAG if/when needed (not part of
    this initial mapping sync)
- **Code touchpoints**:
  `utils/python_callable_method.py` (deletion of both functions),
  `map_firm_dag.py` (deletion of task + import + graph wires +
  docstring update).
- **Future work, if VP→QBO push is needed**: implement as a
  separate trigger-style DAG that mirrors
  `014_503_psa_vantagepoint_customer_to_quickbooks`: lookup
  `map_firm` by VP `ClientID`, then `update_customer` (PUT) or
  `create_customer` (POST). Per-firm only — never a bulk scan.

---

## 11. VEAccounting upsert PUT failing with `Please provide a Vendor Type for table Firm`

- **Symptom**: `process_qbo_firms` succeeded on 136 records but
  failed on 26 vendors — all of them on the
  `PUT /api/firm/{ClientID}` call with body
  `{"VEAccounting": [{"PayTerms": "Next", "Req1099": "N"}]}`. Each
  failure surfaced VP's
  `Failed with error: Please provide a Vendor Type for table Firm`
  message. Names included `Bob's Burger Joint`, `Books by Bessie`,
  `Brosnahan Insurance Agency`, ..., `United States Treasury`.
- **Root cause**: VP re-validates required firm-level fields on
  every PUT to `/api/firm/{ClientID}`, **even** when the body only
  touches the `VEAccounting` sub-resource. The 26 affected vendor
  firms exist in the trial tenant with no `Category` set (VP's UI
  label for this field is "Vendor Type"). They were likely
  seeded into VP outside the integration or by a prior partial
  run that completed firm creation without `Category` populated.
  Our PUT body shape matches the Workato recipe
  `014_503_psa_dvp_insert_update_veaccounting` (line 1822-1825)
  exactly — both send only `PayTerms` and `Req1099` on the update
  path. The Workato `014_503_psa_synch_firms` recipe sets
  `Category` at firm-create time from the account property
  `014_503_PSA.CFG_DefaultVendorType`, so this case only arises
  for VP firms that pre-existed in the tenant without `Category`.
- **Fix applied (Option A — proactive backfill from Airflow
  Variable)**: in `_resolve_vp_firm_for_veaccounting`, also return
  the firm root's `Category`. In the vendor branch of
  `_process_one`, if `vp_vendor_type` is empty, read
  `lookup_default_vendor_type(instance)` (Airflow Variable
  `vp_qbo_mapping_sync_default_vendor_type_<instance>` — the
  Airflow equivalent of Workato's `014_503_PSA.CFG_DefaultVendorType`
  account property) and do a remediating
  `PUT /api/firm/{ClientID}` with `{"Category": <default>}` before
  the VEAccounting upsert. Increment a
  `summary['backfilled_vendor_type']` counter. If the Variable is
  also unset, fall through and let the VEAccounting PUT raise
  VP's original "Please provide a Vendor Type" error — same
  outcome as Workato with `CFG_DefaultVendorType` unset (an
  actionable tenant config gap rather than a silent skip).

  **Revision history**: this entry was first applied as Option B
  (skip + warn + `summary['skipped_no_vendor_type']` counter)
  under a misreading of the recipes. After re-reading
  `014_503_psa_synch_firms` it became clear Workato relies on the
  `CFG_DefaultVendorType` account property to always populate
  `Category` at create time, so the right alignment is to
  populate `Category` from the Variable equivalent rather than
  skip. The skip behaviour is preserved below as a deferred
  alternative.

- **Workato reference**:
  - `014_503_psa_dvp_insert_update_veaccounting.recipe.json`
    line 1822-1825 (PUT body shape — no `Category` on update,
    consistent with our PUT body)
  - `014_503_psa_synch_firms.recipe.json` —
    `014_503_PSA.CFG_DefaultVendorType` account property populates
    `Category` on firm create; that property is the canonical
    source for the per-tenant default vendor type
  - `014_503_psa_quickbooks_customer_vendor_to_vantagepoint.recipe.json`
    line 822-828 (confirms field is named `Category` with UI label
    "Vendor Type")
  - `014_503_psa_firms_and_employees_report.recipe.json`
    line 387-400 (same name/label mapping in the report schema)
- **Code touchpoints**:
  - `utils/python_callable_method.py:_resolve_vp_firm_for_veaccounting`
    — extracts `Category` from firm root, returns it as 3rd
    tuple element; docstring `Returns` updated.
  - `utils/python_callable_method.py:sync_qbo_firms_to_vp._process_one`
    (vendor branch) — when `vp_vendor_type` is missing, does
    a remediating `PUT /api/firm/{ClientID}` with `{"Category":
    lookup_default_vendor_type(instance)}` (gated on the Variable
    being set), then proceeds with the VEAccounting upsert.
  - `utils/python_callable_method.py:sync_qbo_firms_to_vp` —
    `summary` dict gains the `backfilled_vendor_type` counter.

### Future-revisit alternatives (intentionally not applied)

- **B. Skip + warn + counter** when `vp_vendor_type` is missing
  (the original misapplied fix). Replaces the backfill PUT with
  `log.warning` and a `skipped_no_vendor_type` counter; the
  VEAccounting upsert is skipped entirely for those firms. Doesn't
  fix the underlying data — those firms remain unable to receive
  accounting updates until manually fixed. Reasonable choice only
  if backfilling Category by automation is not desired (e.g. some
  tenants require manual review before assigning a vendor
  category).
- **C. Reactive retry on the specific error**: catch the
  "Please provide a Vendor Type" exception from the VEAccounting
  PUT, do the same remediating Category PUT as in the applied
  fix, then retry. Same end state as Option A but pays an extra
  failed PUT round-trip per affected firm; slightly brittle on
  error-message matching.
- **D. Pure config + manual data fix**: set the Variable in the
  tenant env AND manually set Category on each affected firm via
  the VP UI. Zero code change; one-off manual effort. Doesn't
  generalise to other tenants where seeded firms lack Category.

---

## 12. Address + contact POST duplicating on forced re-sync

- **Symptom**: every forced re-sync (operator deletes the
  `vp_qbo_mapping_init_<customerId>_<instance>` Variable, or
  `CFG_UpgradeDataSync='true'` flips `mapping_table_state.Status`
  back to '') appends a new address row + contact row to every
  firm that already has them in VP. Over a few iterations the VP
  firm record accumulates duplicate-but-not-identical address/
  contact entries.
- **Root cause**: in `sync_qbo_firms_to_vp._process_one`, the
  address POST + contact POST sub-resource calls sat **after**
  the create/update if/else block — at the same indent level — so
  they fired on **both** branches. The create branch correctly
  needed the POST (new firm has no sub-resources yet); the update
  branch should have either upserted (read existing → match by
  QBOID → PUT) or skipped, but instead blindly POSTed a
  duplicate. The duplicate accumulation only matters on forced
  re-sync because successful runs flip the init Variable to
  `'true'` and the dispatcher's skip gate short-circuits
  subsequent dispatcher invocations.
- **Surfaced by**: review-bot flag during the BatchTaskRunOperator
  wrap rollout. "Upserts beat blind creates. Flag PUT/POST flows
  that don't check for existence first when the source data may
  already exist in the target." — see code-review comment thread
  on MAP2-3312.
- **Fix applied (minimum — gate behind create)**: move both
  `VantagepointFirmAddressOperator` and `VantagepointContactOperator`
  POST calls **inside** the `else:` block of the create-vs-update
  if/else, so they only fire when the firm itself was just
  created. Re-sync runs that hit the update branch leave the
  existing address/contact untouched.
- **Workato reference**:
  - `014_503_psa_synch_firms.recipe.json` — POSTs address and
    contact only on the first sync; subsequent runs see the
    Workato lookup-table `014_503_psa_map_firm` already populated
    and skip the entire firm processing block.
  - `014_503_psa_dvp_insert_update_veaccounting.recipe.json` —
    the canonical upsert shape this fix log frequently
    references (GET sub-resource → branch on emptiness → POST or
    PUT). Address / contact don't yet follow that shape; see the
    TODO below.
- **Code touchpoints**:
  - `utils/python_callable_method.py:sync_qbo_firms_to_vp._process_one`
    — both POST blocks moved inside the create `else:` branch
    (indent-level shift). Inline `TODO(MAP2-XXXX)` comment names
    the deferred upsert work.
- **Trade-off / deferred work**: gating behind create stops the
  duplicate accumulation but ALSO means QBO-side address /
  contact edits never propagate to VP after the firm's first
  sync. Acceptable for now because:
    1. The Workato source-of-truth recipes have the same shape
       — they POST only on first sync. This fix restores that
       parity.
    2. Address/contact updates from QBO are rare on mature
       tenants; if it becomes a real complaint, the proper
       upsert (GET firm sub-resource by QBOID → match → PUT or
       no-op) is mechanically the same shape as
       `_resolve_vp_firm_for_veaccounting` already implements
       for VendorAccountingInfo.

### Future-revisit alternatives (intentionally not applied)

- **B. Proper upsert via existing-resource lookup**. Mirror the
  VendorAccountingInfo upsert in
  `_resolve_vp_firm_for_veaccounting`:
    - `GET /api/firm/{client_id}/address`, match candidate
      addresses by QBOID (or `PrimaryInd='true'` if QBOID isn't
      stamped on the sub-resource), then `PUT` with the stored
      primary key when found, `POST` otherwise.
    - Same for `GET /api/firm/{client_id}/contact` matched by
      QBOID (which `build_vp_firm_contact_body_from_qbo` already
      stamps on the body).
  Pulls more VP API round-trips per firm (one GET per
  sub-resource) but propagates QBO-side edits correctly.
- **C. Switch to PUT-only on update branch**. Skip the GET,
  assume one address / one contact per firm, PUT to a
  conventional `/{client_id}/address/primary` style endpoint if
  VP exposes it. Smaller round-trip cost than (B) but relies on
  VP API conventions we'd need to verify per endpoint.

---

## 13. Forward sync force-created a VP firm for every QBO entity (FirmID populated where Workato leaves it blank) + duplicate rows on re-sync

- **Symptom**: comparing the Airflow `map_firm` export against the
  Workato `014_503_psa_map_firm` lookup table showed two divergences:
  1. **Duplicate rows** — every firm appeared ~twice (a 266-row export
     = two concatenated full sync passes, 123 + 143 rows).
  2. **FirmID populated everywhere** — Airflow had a VP ClientID for
     every active QBO customer/vendor, including QBO-native / sample
     entities (Amy's Bird Sanctuary, Acme Corp, …) that Workato
     deliberately leaves with a blank FirmID.
- **Root cause (duplicates)**: `map_firm` was created with no
  UNIQUE/PRIMARY KEY, so `_upsert_map_firm_row`'s `INSERT OR REPLACE`
  degraded to a plain `INSERT`; forced re-syncs stacked full copies.
- **Root cause (FirmID)**: `sync_qbo_firms_to_vp` unconditionally
  `POST`ed a new VP firm for every active QBO record on the create
  branch, then wrote the returned ClientID — so no row was ever blank,
  and VP was polluted with firms Workato never created. Workato's
  `014_503_psa_synch_firms` instead only fills ClientID for entities
  that already resolve to a VP firm (recipe step 11 filters
  `WHERE mf.QBOID != '' AND mf.ClientID = ''`); QBO-native entities stay
  unmapped.
- **Fix applied (duplicates)**: extended
  `S3CreateMultiTableCollectionOperator` with a per-table
  `unique_columns` spec (creates a UNIQUE index idempotently, de-duping
  an already-populated table in place first); `map_firm` declares
  `unique_columns=['QBOID', 'IsVendor']`
  (`common.tables.MAP_FIRM_UNIQUE_COLUMNS`) via
  `dispatcher_dag.init_mapping_collections`. `INSERT OR REPLACE` now
  upserts on that key.
- **Fix applied (FirmID — Workato parity)**: the create branch was
  removed. `sync_qbo_firms_to_vp` is now map-only:
    - QBO entity resolves to a VP firm (local row or VP-by-QBOID index)
      → PUT update (+ VEAccounting upsert for vendors), row written with
      the VP ClientID.
    - Otherwise → row written with a **blank FirmID** (unmapped); no VP
      firm is created.
  The `build_vp_firm_create_body_from_qbo` /
  `build_vp_firm_address_body_from_qbo` /
  `build_vp_firm_contact_body_from_qbo` builders are retained (documented
  field shapes) but no longer invoked by the forward sync.
- **Validator follow-through**: `_validate_map_firm_with_cursor`
  reclassified blank FirmID from `hard_fail (missing_firm_id)` to an
  informational `warn (unmapped_firm)` — a blank FirmID is now a valid
  tracked state, so only a missing QBOID hard-fails.
- **Code touchpoints**:
  - `rail .../s3_create_multi_table_collection_operator.py` —
    `unique_columns` support + `_ensure_unique_index` (dedupe-then-index).
  - `common/tables.py` — `MAP_FIRM_UNIQUE_COLUMNS`.
  - `mapping_sync/dispatcher_dag.py` — pass `unique_columns` for map_firm.
  - `utils/_firm_sync.py:sync_qbo_firms_to_vp._process_one` — create
    branch replaced with the unmapped (blank-FirmID) branch; summary
    counter `created` → `unmapped`.
  - `utils/_validate.py:_validate_map_firm_with_cursor` — FirmID check
    downgraded to warn.
- **Migration note**: on the next dispatcher run an existing tenant's
  duplicated `map_firm` is de-duplicated in place (most-recent row per
  key kept) and gains the UNIQUE index — no DROP, so the step-status
  skip-gate can't leave the table empty.

---

## General lessons / patterns

- **VP API quirks worth remembering**:
  - Country master list rejects common aliases (`USA`, `US`,
    etc.) — normalize before sending.
  - VP rewrites stored identifiers via tenant numbering rules
    (e.g. zero-pads vendor codes). Read back the VP-stored value
    via GET; don't trust the value you just sent on POST/PUT.
  - The Vision endpoint
    (`/vantagepointinternal/vision/firm/...`) is separate from
    `/api/firm/...`. Use the right one per recipe.
  - "Object reference not set to an instance of an object" is
    VP's catch-all NPE — almost always means a required
    relation-keyed field is null on the deserialized entity.
    Check the recipe schema for what fields are sent.
  - Sub-resources (address, contact, vendorAccountingInfo) are
    accessed via `/api/firm/{ClientID}/{resource}`. Firm root
    response doesn't reliably include them.
- **When in doubt**: the Workato recipes under
  `integration_vantagepoint_quickbooks/code/014-503 PSA/` are the
  source of truth for endpoint paths, verbs, and body shapes.
  `vendor_sync/utils/python_callable_method.py` is the
  battle-tested Python translation — port from there rather than
  inventing.
- **Upsert pattern**: every "POST then NPE" failure in this list
  ended up being a missed upsert. Default to: GET first, branch on
  what's there. Workato recipes are upsert-shaped by convention;
  mirror that shape.

---

## Diagnostic shortcuts

When `process_qbo_firms` fails next time, the fastest path is:

1. Grep the log for `Failed with error:` — distinct values give
   you the failure classes.
2. Trace each error back to the last `Calling HTTP method ...
   with body ...` line above it — that's the request that
   triggered VP's complaint.
3. Cross-check the request body / URL against the relevant
   Workato recipe under
   `integration_vantagepoint_quickbooks/code/014-503 PSA/`. The
   recipe `path` and `data` fields show what VP actually expects.
4. Cross-check against `vendor_sync/utils/python_callable_method.py`
   helpers — `build_*_body` functions there are already
   recipe-faithful and have hard-learned comments in their
   docstrings.

If the error string starts with `Object reference not set to an
instance of an object`, it's almost always one of:

- A required relation-keyed field is missing or set to the wrong
  identifier (fix shape: GET the parent record first, read back
  what VP stored, use that).
- The recipe is upsert-shaped and you're hitting the insert path
  when the entity already exists (fix shape: add a pre-check GET
  on the dedicated sub-resource endpoint, branch on emptiness).
