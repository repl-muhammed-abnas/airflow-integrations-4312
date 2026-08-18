# `map_employee` sync — fix log

A running log of the bugs hit in `process_qbo_employees` (the
python_callable that powers `sync_qbo_employees_to_vp` in
`utils/python_callable_method.py`) during trial runs against the
deployed Vantagepoint tenant, and what the fix ended up being. Sibling
log: `MAP_FIRM_SYNC_FIX_LOG.md`.

Each entry is structured:

- **Symptom** — the exact error message or behavior observed
- **Root cause** — why it happened
- **Fix** — what changed in code
- **Workato reference** — the canonical Workato recipe / vendor_sync
  helper that informed the fix
- **Code touchpoints** — where the fix lives now

---

## 1. POST `/employee` rejected with `Field EmplQBOID does not exist`

- **Symptom**: `process_qbo_employees` POST `/api/employee` failed for
  every employee whose `Employee` value passed VP's primary-key format
  check (e.g. Emily Platt, qbo_id `55`; John Johnson, qbo_id `54`)
  with `Failed with error: Field EmplQBOID does not exist`. The POST
  body included `"EmplQBOID": "55"`.
- **Root cause**: `build_vp_employee_create_body_from_qbo` was sending
  `EmplQBOID` as a VP field name. VP's actual field for the QBO
  cross-reference on the Employee table is `QBOID` — the same name
  used on the Firm table. The misnaming came from the Workato recipe's
  *parameter* `EmplQBOID` (`014_503_psa_vantagepoint_upsert_employee.recipe.json`
  lines 76-77 input schema), which the recipe immediately remaps to
  the VP field `QBOID` when posting (lines 1016-1017, 2200):
  ```
  "Employee": "#{...EmplQBOID}"
  "QBOID":    "#{...EmplQBOID}"
  ```
  Our code skipped that remap and used the parameter name verbatim as
  the VP field name.
- **Fix**: renamed `EmplQBOID` → `QBOID` in
  `build_vp_employee_create_body_from_qbo`. The update body builder
  (`build_vp_employee_update_body_from_qbo`) reuses the create body,
  so both paths are fixed in one edit.
- **Workato reference**:
  `014_503_psa_vantagepoint_upsert_employee.recipe.json` lines 1016-1017
  (POST body) + 2200 (PUT body) — both use VP field `QBOID`, not
  `EmplQBOID`.
- **Code touchpoints**:
  `utils/python_callable_method.py:build_vp_employee_create_body_from_qbo`.

---

## 2. POST `/employee` rejected with `"<id>" is invalid. The correct format is XXXX`

- **Symptom**: `process_qbo_employees` POST `/api/employee` failed for
  three QBO employees whose `Id` was 9 digits (`400000001`,
  `400000011`, `400000021` — names `Test Employee`, `Test Post
  Employee`, `Test VP_QBO`) with `Failed with error: "400000001" is
  invalid. The correct format is XXXX`. The POST body included
  `"Employee": "400000001"`.
- **Root cause**: VP's `Employee` primary-key field has a
  tenant-enforced max length (the error message's `XXXX` template
  means "up to 4 characters" — VP accepts 2-character values like
  `55` without complaint). The Workato recipe and our code both pass
  the QBO `Id` verbatim as the VP `Employee` field. This works in
  production tenants where QBO Employee Ids stay short, but fails
  here where some QBO records have 9-digit Ids.
- **Fix (Option 2a — skip + warn, applied)**: in
  `sync_qbo_employees_to_vp`, before the per-employee try block, if
  `len(str(qbo_id)) > 4` log a `WARNING` and increment a new
  `summary['skipped_employee_id_too_long']` counter, then `continue`.
  The map_employee row is **not** written for these records (no VP
  Employee was created). The task no longer fails for this class of
  data.
- **Workato reference**: the recipe doesn't handle this case —
  `014_503_psa_vantagepoint_upsert_employee.recipe.json` line 1016
  passes the parameter `EmplQBOID` directly into the VP `Employee`
  field with no length check. Workato relies on the assumption that
  QBO Employee Ids stay short in production tenants.
- **Code touchpoints**:
  - `utils/python_callable_method.py:sync_qbo_employees_to_vp` —
    summary dict gains the `skipped_employee_id_too_long` counter.
  - `utils/python_callable_method.py:sync_qbo_employees_to_vp` —
    per-employee loop gains the length-check guard right after the
    inactive-skip guard.

### Future-revisit alternatives (intentionally not applied)

If these long-Id employees ever need to actually flow into VP
(rather than be skipped), the alternatives considered and deferred:

- **2b. Omit `Employee` from POST body when `len(qbo_id) > 4`, let VP
  autonumber.** Capture the VP-assigned `Employee` from the POST
  response via the existing `_extract_vp_employee_id` helper, then
  write the map_employee row using that VP-assigned key. Self-healing.
  Assumes VP autonumber on the Employee field is enabled in the
  tenant; if it's not, the POST still fails with a different error
  ("Please provide an Employee") and these records still need
  handling.
- **2c. Try 2b first, fall back to 2a** on the autonumber-disabled
  error. Belt-and-braces; most code; extra failed POST on tenants
  where autonumber is off.

---

## 3. VP field name corrections: `EMail`, `Salutation`, `ZIP`

- **Symptom**: silent — would not have surfaced on Emily/John in the
  trial tenant because their QBO records lacked email/title/zip. Found
  by recipe diff during alignment review for issue #5 below.
- **Root cause**: three VP API field names were mistyped in the body
  builder:
  - we sent `Email` — VP field is `EMail` (capital M)
  - we sent `Title` — VP field is `Salutation` (recipe sources it from
    the upsert's `Title` parameter at line 1025)
  - we sent `Zip` — VP field is `ZIP` (all caps)
  VP silently ignores unknown fields rather than rejecting, so each of
  these would have caused the corresponding QBO data to never land in
  VP without any visible error.
- **Fix**: renamed in the rewritten body builders (see #5 below).
- **Workato reference**:
  `014_503_psa_vantagepoint_upsert_employee.recipe.json` lines
  1013 (`EMail`), 1025 (`Salutation` sourced from `Title` param),
  1031 (`ZIP`).
- **Code touchpoints**:
  `utils/python_callable_method.py:build_vp_employee_create_body_from_qbo`
  (and the update builder, which derives from it).

---

## 4. Missing `ReadyForProcessing` and `ReadyForApproval` flags

- **Symptom**: silent — VP appears to accept POST/PUT without these
  fields in some tenant configurations. Found by recipe diff.
- **Root cause**: the Workato `upsert_employee` recipe hardcodes
  `ReadyForProcessing: "true"` and `ReadyForApproval: "true"` on both
  POST (lines 1008-1009) and PUT (lines 2192-2193). Our body builders
  never sent these fields, so VP fell back to its own per-field
  defaults — which may or may not match what downstream VP workflows
  expect (e.g. employees created via the integration not appearing in
  "ready for processing" queues until manually flipped).
- **Fix**: hardcoded `'ReadyForProcessing': 'true'` and
  `'ReadyForApproval': 'true'` in the rewritten create body (and
  inherited by the update body builder).
- **Workato reference**:
  `014_503_psa_vantagepoint_upsert_employee.recipe.json` POST lines
  1008-1009 + PUT lines 2192-2193.
- **Code touchpoints**:
  `utils/python_callable_method.py:build_vp_employee_create_body_from_qbo`.

---

## 5. POST `/employee` rejected with `Organization is required` + strict `synch_employees` body parity

- **Symptom**: `process_qbo_employees` POST `/api/employee` failed for
  Emily Platt (qbo_id `55`) and John Johnson (qbo_id `54`) with
  `Failed with error: Organization is required. Organization is
  required.` The POST body included no `Org` field (the per-instance
  Airflow Variable `vp_qbo_mapping_sync_default_organization_trial`
  was unset, so `lookup_default_organization()` returned None and
  `_filter_none` dropped the key).
- **Root cause**: two related problems became visible at once:
  1. Our body builder had no fallback when `lookup_default_organization`
     returned None — it just dropped `Org`. The Workato
     `upsert_employee` recipe has a built-in VP-side fallback for this
     case (recipe lines 1010 / 2194):
     ```
     param.Organization.presence
       || data.deltek_vantagepoint_connector.82b250d3.organizations.first.Org.presence
       || blank
     ```
     i.e. when no parameter is passed, the recipe queries
     `GET /api/organization` (recipe action at line 446-460) and uses
     the first row's `Org` value. We never replicated that fallback.
  2. Our body builder was sending a much richer body than `synch_employees`
     actually passes. The Workato initial-sync recipe
     `014_503_psa_synch_employees.recipe.json` (lines 823-828) passes
     only four parameters to upsert_employee:
     ```
     FirstName, MiddleName, LastName, EmplQBOID
     ```
     All other fields (HireDate, addresses, phones, EMail, Salutation,
     Suffix, EmployeeCompany, HomeCompany, Organization, Status,
     TerminationDate) are left blank — they resolve to `""` or `null`
     in the actual POST body. Our code populated all of these from the
     QBO record at create time, which is **out of scope for initial
     sync** in the Workato design (richer data flows via the per-employee
     polling recipe `014_503_psa_employee_upserted_in_vantagepoint`,
     which would be a separate trigger DAG here if/when needed —
     intentionally not implemented yet, same pattern as the firm
     reverse-sync removal in MAP_FIRM_SYNC_FIX_LOG.md #10).
- **Fix**: rewrote both body builders for **strict synch_employees
  parity**. The new create body contains only the fields that the
  Workato POST actually populates with concrete values:
  - `ReadyForProcessing: "true"`, `ReadyForApproval: "true"` (hardcoded — see #4)
  - `Org`: `lookup_default_organization(instance)` → `vp_default_org`
    (from new `_fetch_first_vp_organization_org` helper) → `""`
  - `HomeCompany: ""`, `EmployeeCompany: ""`, `EMail: ""` (recipe sends
    blank when no param passed — no VP fallback for these three;
    confirmed by reading recipe lines 1011-1013 carefully)
  - `LastName`, `FirstName`, `Employee`, `QBOID` from QBO data
  - `Type`: `lookup_default_employee_labor_type(instance)` → `""`
    (recipe equivalent: Workato account property
    `CFG_DefaultEmployeeLaborType` at recipe line 1018)
  - `Status: "A"` (recipe defaults this when no param — confirmed by
    `param.Status.presence || "A"` at line 1019; we already skip
    inactive employees so `"A"` is always correct for records we sync)
  The new update body is the create body minus `QBOID` and `Type`
  (recipe PUT excludes both — lines 2194-2215). Also corrected the
  prior code's incorrect `pop('HomeCompany')` — recipe PUT does
  include `HomeCompany`.
  A new helper `_fetch_first_vp_organization_org(vp_conn_id, context)`
  performs the `GET /api/organization` fallback. `sync_qbo_employees_to_vp`
  calls it once at the top of the function and passes the cached
  result into both body builders, avoiding N+1 VP queries in the bulk
  loop.
- **Workato reference**:
  - `014_503_psa_synch_employees.recipe.json` lines 823-828 — the
    minimal parameter set passed to upsert
  - `014_503_psa_vantagepoint_upsert_employee.recipe.json`:
    - POST body lines 1006-1034
    - PUT body lines 2194-2215
    - Org fallback expression lines 1010 and 2194
    - GET /api/organization action lines 446-460
    - hardcoded `ReadyForProcessing` / `ReadyForApproval` lines
      1008-1009 (POST) and 2192-2193 (PUT)
- **Code touchpoints**:
  - `utils/python_callable_method.py:_fetch_first_vp_organization_org`
    (new helper)
  - `utils/python_callable_method.py:build_vp_employee_create_body_from_qbo`
    (rewritten — new signature with `vp_default_org` parameter)
  - `utils/python_callable_method.py:build_vp_employee_update_body_from_qbo`
    (rewritten — derives from create body, drops `QBOID` and `Type`,
    new signature with `vp_default_org` parameter)
  - `utils/python_callable_method.py:sync_qbo_employees_to_vp` —
    fetches `vp_default_org` once near the start; passes it to both
    body builders at their callsites
- **Behaviour change at runtime**: QBO data we previously sent
  (HireDate, Email, addresses, phones, TerminationDate, Title) will
  no longer flow into VP at create time. This is intentional and
  matches Workato. If/when this data fidelity is needed, see
  the deferred per-employee polling DAG mentioned in the root-cause
  point #2 above.

### Future-revisit alternatives (intentionally not applied)

- **5a. Richer body shape — preserve QBO data while keeping recipe
  field names.** Send `EMail`, `Salutation`, `ZIP`, addresses,
  phones, hire/termination dates at create time, in addition to the
  minimal-X body. Matches the recipe's field names but goes beyond
  what `synch_employees` does. Would require keeping all the
  per-field QBO lookups in the body builder. Useful if richer data
  in VP at first sync is required and the per-employee polling DAG
  isn't planned.
- **5b. Per-employee polling DAG mirroring
  `014_503_psa_employee_upserted_in_vantagepoint`.** Separate DAG
  (not part of mapping_sync) that picks up changed QBO employees and
  pushes the richer body shape. Same pattern as the firm
  reverse-sync removal in MAP_FIRM_SYNC_FIX_LOG.md #10.

---

## 6. POST `/employee` rejected with `Please provide a Labor Posting Type for table Employees`

- **Symptom**: after the strict synch_employees body rewrite (fix
  #5) and Org fallback were in place, the POST body looked correct:
  ```
  {"ReadyForProcessing": "true", "ReadyForApproval": "true",
   "Org": "00000:0000:000", "HomeCompany": "", "EmployeeCompany": "",
   "EMail": "", "LastName": "Platt", "FirstName": "Emily",
   "Employee": "55", "QBOID": "55", "Type": "", "Status": "A"}
  ```
  but VP still rejected it for Emily Platt and John Johnson with
  `Failed with error: Please provide a Labor Posting Type for table
  Employees`. The body has `"Type": ""`.
- **Root cause**: VP requires a non-empty `Type` (the field's UI
  label is "Labor Posting Type"). The Workato `upsert_employee`
  recipe reads `Type` from the account property
  `014_503_PSA.CFG_DefaultEmployeeLaborType` (recipe line 1018) —
  no VP-side fallback (unlike `Org` at line 1010 which has a
  fallback to the first VP organization). The recipe assumes the
  account property is set per tenant during integration deployment;
  when unset, Workato would also fail with the same VP error.
  Our equivalent is the Airflow Variable
  `vp_qbo_mapping_sync_default_employee_labor_type_<instance>`,
  read via `lookup_default_employee_labor_type(instance)`. In the
  trial environment this Variable is not set, so the body builder
  sends `"Type": ""`.
- **Fix applied (Option A1 — configuration only, no code change)**:
  set the Airflow Variable per tenant. Matches Workato exactly
  (Workato also requires the account property to be set; this
  Variable is the Airflow-side equivalent). No code skip, no
  fallback PUT — strict Workato parity. The trial tenant needs
  `vp_qbo_mapping_sync_default_employee_labor_type_trial` set to
  the tenant's correct VP labor-type code; subsequent tenants need
  the equivalent Variable set during onboarding.
- **Why no proactive backfill (contrast with `map_firm` fix #11
  revision)**: the firm-side backfill (PUT `Category` from the
  Variable) only made sense because firms can pre-exist in VP
  without `Category` (seeded externally / by prior partial runs).
  Employees in the mapping_sync flow are always *created fresh by
  us* — there's no "pre-existing employee with missing Type"
  scenario. The Variable simply feeds the create body; if it's
  unset the configuration gap is loud and actionable rather than
  silently patched over.
- **Workato reference**:
  - `014_503_psa_vantagepoint_upsert_employee.recipe.json` line
    1018 — `Type` reads from the Workato account property
    `014_503_PSA.CFG_DefaultEmployeeLaborType`, no fallback.
- **Code touchpoints**: none for the fix itself. The Variable
  read site is `utils/python_callable_method.py:lookup_default_employee_labor_type`
  and its use in `build_vp_employee_create_body_from_qbo`
  (entry #5).

### Future-revisit alternatives (intentionally not applied)

- **A2. Skip + warn + counter** when
  `lookup_default_employee_labor_type(instance)` is None: matches
  the project's other skip-edge-case patterns (e.g. firm fix #11's
  initial misapplied Option B before revision) but deviates from
  Workato. Useful only if some tenants are expected to operate
  without a default labor type and we want partial-success rather
  than fail-loud.
- **A3. Hard-fail-early at task entry**: check the Variable at the
  top of `sync_qbo_employees_to_vp`; if None, raise immediately
  with an actionable "Variable not configured" message before
  iterating. Loud but stops the whole task on one config gap.

---

## 7. POST `/employee` rejected with `Record 00000|<id> already exists and cannot be added`

- **Symptom**: `process_qbo_employees` POST `/api/employee` failed for
  Emily Platt (qbo_id `55`) and John Johnson (qbo_id `54`) with
  `Failed with error: Record 00000|0055 already exists and cannot be
  added. Record 00000|0055 already exists and cannot be added.` (and
  the analogous error for `0054`). The POST body looked correct
  post-fix-#6:
  ```
  {"ReadyForProcessing": "true", "ReadyForApproval": "true",
   "Org": "00000:0000:000", "HomeCompany": "", "EmployeeCompany": "",
   "EMail": "", "LastName": "Platt", "FirstName": "Emily",
   "Employee": "55", "QBOID": "55", "Type": "E", "Status": "A"}
  ```
- **Root cause**: `map_employee` starts empty on a fresh tenant, so
  `_load_existing_map_employee_index` returns an empty dict and the
  per-employee branch defaulted to POST for every QBO record. The
  VP tenant already had employees with QBOID 54 and 55 (seeded by a
  prior Workato run / manual import / earlier failed run), so VP
  rejected the POST as a duplicate. The Workato design avoids this
  case via a separate pre-population recipe
  (`014_503_psa_map_employees.recipe.json`) that runs *before*
  `synch_employees` and seeds the map_employee lookup from
  `GET vision/QuickBooks/Employees` (a VP custom endpoint that
  returns existing VP employees with their QBO cross-references).
  Subsequent `synch_employees` SQL filters with
  `WHERE me.QBOID != '' AND Employee = ''`, so already-mapped
  employees never hit the upsert path. We had no equivalent seeding
  step.
- **Fix**: per-record VP lookup before deciding POST vs PUT,
  mirroring `_find_vp_firm_by_qbo_id` (added in map_firm fix #5).
  New helper `_find_vp_employee_by_qbo_id(qbo_id, vp_conn_id,
  context)` issues `GET /api/employee?filterHash[0][name]=QBOID&
  filterHash[0][value]=<id>` and returns the VP employee dict on a
  match. In `sync_qbo_employees_to_vp`, when the local
  `existing_map` has no row for the QBOID, the helper is consulted;
  on a VP hit, `existing` is synthesized with the VP `Employee` key
  so the existing PUT branch runs and `_upsert_map_employee_row`
  backfills the local map at the end of the loop. A new
  `summary['backfilled_from_vp']` counter tracks how many records
  took this path.
- **Why per-record instead of the Workato bulk pre-population**:
  the codebase already follows the per-record pattern for firms
  (`_find_vp_firm_by_qbo_id`), and the standard
  `VantagepointEmployeeOperator` supports the `filterHash` GET
  syntax — no new custom-endpoint operator dependency. Bulk
  pre-population via `vision/QuickBooks/Employees` would be more
  efficient on tenants with many employees but adds a new task
  shape (separate seed task in `map_employee_dag.py`). Deferred as
  alternative 7b below.
- **Workato reference**:
  - `014_503_psa_map_employees.recipe.json` — full pre-population
    recipe (bulk approach). The interesting parts:
    - Step 1 (`get_entries` on map_employee): exits early if map
      already has rows.
    - Step 8 (`custom_action`): `GET vision/QuickBooks/Employees`
      with the per-row schema (`Employee`, `QBOID`, `QBOVendorID`,
      ...).
    - Subsequent steps iterate the response and `add_entry` each
      row to the map_employee lookup.
  - `014_503_psa_vantagepoint_upsert_employee.recipe.json` step 3
    (`search_entries f11ad910`): per-record map lookup by QBOID
    that decides POST vs PUT (line 977 — POST iff entries.____Size
    == 0 or first entry's `col1`/Employee is blank).
- **Code touchpoints**:
  - `utils/python_callable_method.py:_find_vp_employee_by_qbo_id`
    (new helper).
  - `utils/python_callable_method.py:sync_qbo_employees_to_vp` —
    summary dict gains `backfilled_from_vp`; per-employee loop
    consults the helper when the local map lookup misses.

### Future-revisit alternatives (intentionally not applied)

- **7a. Catch the `already exists` error and follow up with the
  lookup.** Let the POST go first, parse the error string, then do
  the GET. Saves one VP call per record on the steady-state path
  (no VP-side duplicate). More fragile — depends on the exact VP
  error wording. The per-record GET is cheap enough that the
  upfront cost isn't worth the brittleness.
- **7b. Bulk pre-population via `GET vision/QuickBooks/Employees`,
  matching Workato exactly.** Add a `seed_map_employee_from_vp`
  task to `map_employee_dag.py` upstream of
  `process_qbo_employees`. One VP call, regardless of employee
  count. Adds a custom-endpoint operator and a new task. Reach for
  this if employee counts grow large enough that N per-record GETs
  become a measurable cost.

---

## 8. PUT `/employee/<id>` rejected with `Please provide a Employee Company Name … Please provide a Home Company`

- **Symptom**: after fix #7 (VP backfill helper), the GET-by-QBOID
  step correctly found existing VP employees 0054 and 0055 and
  routed them to the PUT branch. The PUT body looked like a strict
  recipe-parity update body, but VP rejected with:
  ```
  Failed with error: Please provide a Employee Company Name for
  table Employees<BR/>Please provide a Home Company for table
  Employees …
  ```
  The PUT body included `"HomeCompany": ""` and
  `"EmployeeCompany": ""`. POST with the same empty values had
  worked earlier in the migration for net-new employees.
- **Root cause**: VP's `/employee` endpoint validates `HomeCompany`
  and `EmployeeCompany` differently on POST vs PUT. On POST (new
  record) it accepts empty strings (defaults to tenant-wide blanks).
  On PUT it requires non-empty values for **existing** records — an
  empty string reads as "clear this required field" and is rejected.
  Cross-checking the Workato recipe shows the PUT body at lines
  2195-2197 uses the `.presence || blank` expression for
  `HomeCompany`, `EmployeeCompany`, `EMail`. In Workato's expression
  language `blank` returns nil; the HTTP layer drops nil-valued
  keys from the JSON body. So the recipe PUT effectively OMITS
  these fields when no param is passed — which lets VP preserve
  whatever the existing record already has. Our `build_vp_employee_
  update_body_from_qbo` derived from the create body builder
  (correct for the field list) but sent `""` literals, not omissions.
- **Fix**: drop empty-string values for `HomeCompany`,
  `EmployeeCompany`, and `EMail` from the update body before
  returning. The create body still sends `""` (matches recipe POST
  behavior and works fine for new records). The update builder
  pops the keys when value is exactly `''`. VP then sees no value
  in the body for these fields and preserves the existing record's
  values.
- **Workato reference**:
  `014_503_psa_vantagepoint_upsert_employee.recipe.json` PUT body
  lines 2195-2197 — three `.presence || blank` expressions. The
  POST body at lines 1011-1013 uses the same pattern but VP
  accepts empty values on POST per the validation difference
  above.
- **Why not look up the existing values from the GET response**:
  the `_find_vp_employee_by_qbo_id` helper does return the full
  VP employee dict including `HomeCompany` / `EmployeeCompany`,
  so we COULD thread that through to the update body and explicit-
  ly send back the same values. Omission is equivalent (VP
  preserves what's there) and simpler — no extra parameter on
  the body builder, no special case for the backfill path vs the
  "existing in map_employee" path (the latter doesn't have the
  VP dict on hand). Matches Workato exactly.
- **Code touchpoints**:
  - `utils/python_callable_method.py:build_vp_employee_update_body_from_qbo`
    — strips `''` values for `HomeCompany`, `EmployeeCompany`,
    `EMail` after deriving from the create body.

### Future-revisit alternatives (intentionally not applied)

- **8a. Preserve from VP**: pass `vp_existing` into the update
  body builder and use `vp_existing.get('HomeCompany')` etc.
  Explicit and defensible — the values we send back are the
  same as what's already there. Adds a parameter and a branch
  (only the backfill path has the VP dict; the local-map path
  doesn't). Equivalent behavior to omission, more code.
- **8b. Fall back to Airflow Variable defaults** on PUT when
  blank: `body['HomeCompany'] = lookup_default_home_company(
  instance) or <existing-value-if-known>`. Useful if the existing
  VP values are stale or wrong, but that's not what mapping_sync
  is trying to fix — initial mapping shouldn't be rewriting
  values that already exist for non-QBO-driven fields.

---

## General notes

- The Workato recipe distinguishes between **parameter names** (free
  to be whatever the recipe author chose, e.g. `EmplQBOID`) and **VP
  API field names** (defined by VP, e.g. `QBOID`). Always read the
  recipe's actual HTTP body block (the `path` + `request_data` /
  `input` near a `quickbooks` or `deltek` provider action), not the
  recipe's parameter input schema, to find the VP field names.
- The per-employee loop is fail-soft: per-record errors land in
  `summary['errors']` and the task only raises at the end. This means
  one bad record doesn't block 100 good ones, but the task still
  fails overall — by design, so the dispatcher's catch task fires
  and the failure is visible.
