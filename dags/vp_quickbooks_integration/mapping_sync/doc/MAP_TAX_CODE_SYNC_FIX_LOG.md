# `map_tax_code` sync — fix log

A running log of the bugs hit in `process_qbo_tax_codes` (the
python_callable that powers `sync_qbo_tax_codes_to_vp` in
`utils/python_callable_method.py`) during trial runs against the
deployed Vantagepoint tenant, and what the fix ended up being.
Siblings: `MAP_FIRM_SYNC_FIX_LOG.md`, `MAP_EMPLOYEE_SYNC_FIX_LOG.md`.

Each entry is structured:

- **Symptom** — the exact error message or behavior observed
- **Root cause** — why it happened
- **Fix** — what changed in code
- **Workato reference** — the canonical Workato recipe / vendor_sync
  helper that informed the fix
- **Code touchpoints** — where the fix lives now

---

## 1. POST `/TaxCodeEntity/` rejected with `Field TaxType does not exist` + strict Workato body parity

- **Symptom**: `process_qbo_tax_codes` POST `/vision/TaxCodeEntity/`
  failed for every flattened (TaxCode, TaxRate) component with
  `Failed with error: Field TaxType does not exist. Field TaxType
  does not exist.` The POST body included `"TaxType": "Sales"` or
  `"TaxType": "Purchase"` and a handful of other extra fields:
  ```
  {"Code": "California-California", "Description": "California",
   "Rate": "8", "TaxType": "Sales", "Status": "A",
   "IsTaxGroup": "Y", "QBOTaxCodeID": "2", "QBOTaxRateID": "3"}
  ```
  All 9 components failed and `map_tax_code` ended the run empty.
- **Root cause**: `build_vp_tax_code_create_body` and the update
  builder were a best-guess sketch — the comment above them said
  "fields are best-effort given the docs don't fully spec the VP
  body. The shape below covers the Workato recipe's typical send
  pattern." When the trial tenant actually rejected `TaxType`, it
  exposed that VP's TaxCodeEntity schema doesn't have that field
  at all. Cross-checking the Workato GL recipe
  `014_503_psa_sync_tax_codes.recipe.json` showed both POST (lines
  3960-3967) and PUT (lines 4866-4873) send a much smaller body
  than we were sending. Specifically, Workato sends:
  - `Description` ← RateName
  - `Code` ← VP code (POST only — PUT puts it in the URL)
  - `Rate` ← rate value
  - `QBOID` ← QBO TaxCode Id (POST only — immutable cross-reference;
    PUT drops it)
  - `QBOLastUpdated` ← `=now`
  - `Status` ← `'A'` or `'I'`

  And does **not** send: `TaxType`, `IsTaxGroup`, `QBOTaxCodeID`,
  `QBOTaxRateID`. The latter three were our invention; the
  TaxType/Sales-vs-Purchase distinction lives at the QBO end and
  doesn't need to round-trip to VP per the recipe design.
- **Fix**: rewrote both body builders for strict Workato parity.
  POST body now contains exactly the 6 recipe fields; PUT body
  contains 4 (drops `QBOID` and `Code` which goes in the URL).
  Added new `_vp_tax_code_now()` helper that returns a UTC ISO 8601
  timestamp — Workato's `=now` equivalent. Stale comment about
  "best-effort" replaced with the recipe citation.
- **Workato reference**:
  `014_503_psa_sync_tax_codes.recipe.json`:
    - POST body lines 3960-3967
    - PUT body lines 4866-4873
    - The foreach `Id` at line 3964 resolves to the QBO TaxCode Id
      (col3 mapping at line 3294 confirms `Id` is the TaxCode Id,
      not the rate Id — `RateId` is camelCase and separately
      mapped to col8 at line 3296).
- **Code touchpoints**:
  - `utils/python_callable_method.py:build_vp_tax_code_create_body`
    (rewritten)
  - `utils/python_callable_method.py:build_vp_tax_code_update_body`
    (rewritten)
  - `utils/python_callable_method.py:_vp_tax_code_now` (new helper)

### Future-revisit alternatives (intentionally not applied)

- **1a. Preserve our extra fields if VP starts accepting them.**
  The original builder included `TaxType`, `IsTaxGroup`,
  `QBOTaxCodeID`, `QBOTaxRateID` on the theory that they're useful
  cross-references. If VP's TaxCodeEntity schema ever adds these
  fields, the symmetric/no-data-loss approach is to put them back.
  Until then, the strict parity build keeps us aligned with Workato
  and avoids the "field does not exist" rejection.
- **1b. Persist Sales-vs-Purchase distinction VP-side via a
  Description suffix or naming convention.** Workato carries
  `TaxOn` in the lookup-table column `col10` (our `map_tax_code.TaxOn`)
  but doesn't push it to VP. Our `_upsert_map_tax_code_row`
  already preserves it locally; if the VP UI ever needs the
  distinction, the right place is `Description` (e.g.
  `"California State (Sales)"`).

---

## 2. PUT path bypassed by `'Vantagepoint Code'` key typo

- **Symptom**: silent — would have surfaced on the SECOND run of
  `process_qbo_tax_codes`. The first run always writes
  `map_tax_code` rows after a successful POST (via
  `_upsert_map_tax_code_row`). On the next run,
  `_load_existing_map_tax_code_index` correctly populates
  `existing_map` with key `'VantagepointCode'`, but
  `sync_qbo_tax_codes_to_vp` checked
  `existing.get('Vantagepoint Code')` (note the space). The check
  always returned None → every row took the POST path again →
  every POST would have failed with VP's duplicate-key error
  (mirror of map_employee fix #7).
- **Root cause**: typo — schema key is `VantagepointCode` (no space,
  per Workato lookup table column label sanitization documented in
  the schema notes near MAP_TAX_CODE_COLUMNS).
- **Fix**: rename in `sync_qbo_tax_codes_to_vp` per-row branch —
  `existing.get('Vantagepoint Code')` → `existing.get('VantagepointCode')`,
  same on the value read line.
- **Workato reference**: n/a — this is a local-typo bug, not a
  recipe divergence. The Workato recipe checks
  `mappedRate.entry.col4` (our `VantagepointCode`) at recipe line
  524 (`014_503_psa_sync_tax_codes.recipe.json`).
- **Code touchpoints**:
  - `utils/python_callable_method.py:sync_qbo_tax_codes_to_vp` —
    per-row PUT-vs-POST branch (around the `existing` check).

---

## 3. POST `/TaxCodeEntity/` rejected with `Tax Code <prefix> already exists` due to Code truncation collisions

- **Symptom**: after fix #1, the POST body looked correct
  (`Description`, `Code`, `Rate`, `QBOID`, `QBOLastUpdated`, `Status`)
  and the first 5 of 9 components created successfully. The
  remaining 4 failed with `Failed with error: Tax Code CA-CONTRA_
  already exists. Tax Code CA-CONTRA_ already exists.` and
  `Tax Code OUT_OF_SCO already exists.` Tracing the bodies:
  ```
  Code: CA-Contra_Costa-Contra_Costa-San_Pablo-California_State           (#2, created OK)
  Code: CA-Contra_Costa-Contra_Costa-San_Pablo-California__Contra_Costa_County         (#3, rejected)
  Code: CA-Contra_Costa-Contra_Costa-San_Pablo-California__Contra_Costa_County_District (#4, rejected)
  Code: CA-Contra_Costa-Contra_Costa-San_Pablo-California__San_Pablo_City_District     (#5, rejected)
  ```
  All four `CA-Contra_…` codes share the prefix `CA-Contra_` (10
  chars). VP uppercases and truncates to `CA-CONTRA_` so #3, #4, #5
  collide with #2. Same pattern for `Out_of_scope-NO_TAX_SALES` vs
  `Out_of_scope-NO_TAX_PURCHASE` (both truncate to `OUT_OF_SCO`).
- **Root cause**: `_vp_tax_code_value` built the VP `Code` as
  `<CodeName>-<RateName>` (sanitized). The VP TaxCodeEntity `Code`
  field is short (~10 chars) and case-insensitive — long derived
  strings collapse to the same prefix and collide. Cross-checking
  the Workato GL recipe surfaced that Workato does **not** derive
  the Code from QBO names. It generates a random short UUID slice:
  ```
  CodeUnique: =workato.uuid.to_s.upcase.slice(0,4)
  ```
  (recipe `014_503_psa_sync_tax_codes.recipe.json` line 3884). The
  POST body sends `CodeUnique` when present, falling back to the
  QBO RateId (recipe line 3962). The generated value is persisted
  in the map_tax_code lookup (`col4`, our `VantagepointCode`) so
  subsequent runs route to PUT with the same code (recipe line
  4618).
- **Fix**: dropped `_vp_tax_code_value` entirely. Added
  `_generate_vp_tax_code()` returning 4 uppercase hex chars from
  `uuid.uuid4().hex`. `sync_qbo_tax_codes_to_vp` now generates the
  code only on the POST path; the PUT path reuses the stored
  `VantagepointCode` from `map_tax_code`. The body builder takes
  `vp_code` as a parameter so the same value can be passed to
  `_upsert_map_tax_code_row` without a second generation. The
  `skipped_no_code` check is repurposed to skip when both CodeName
  and RateName are blank (no `Description` for VP); the prior
  "can't derive code" failure mode is gone.
- **Why hex (16 chars) instead of full alphanumeric (36 chars)**:
  Workato uses `.upcase.slice(0,4)` on a uuid which gives base-16
  hex chars (`0-9A-F`) — 65k combinations. Per-tenant active tax-
  code counts are small enough that collision risk is negligible.
  If a collision ever happens, VP returns the same 'already
  exists' error and the Airflow per-task retry will pick a new
  uuid on the next attempt.
- **Workato reference**:
  - `014_503_psa_sync_tax_codes.recipe.json`:
    - line 3884 — `CodeUnique = workato.uuid.to_s.upcase.slice(0,4)`
    - line 3962 — POST `Code` uses CodeUnique if present, else
      RateId
    - line 4618 — `col4` (VantagepointCode) is persisted to map
      using the same CodeUnique expression
- **Code touchpoints**:
  - `utils/python_callable_method.py:_vp_tax_code_value` (removed)
  - `utils/python_callable_method.py:_generate_vp_tax_code` (new
    helper)
  - `utils/python_callable_method.py:build_vp_tax_code_create_body`
    (new `vp_code` parameter)
  - `utils/python_callable_method.py:sync_qbo_tax_codes_to_vp` —
    generate code only on POST path; reuse stored code on PUT path;
    `skipped_no_code` repurposed for blank-name skip

### Future-revisit alternatives (intentionally not applied)

- **3a. Deterministic codes derived from `(CodeID, RateID)`** (e.g.
  truncated hash, or just `RateID`-padded). Avoids the random-
  collision retry window entirely. Workato chose UUIDs over QBO
  IDs because QBO IDs can collide across tenants if VP cross-
  tenant codes are ever compared, and integer IDs in a `Code`
  field look like other VP records. Mirroring the recipe (UUID) is
  the safer default.
- **3b. Pre-query VP for existing tax codes by QBOID before POST.**
  Same pattern as `_find_vp_employee_by_qbo_id` (employee fix #7)
  / `_find_vp_firm_by_qbo_id`. Would handle the case where VP
  already has a tax code with our QBOID from a prior Workato run.
  Deferred: the trial tenant didn't have pre-existing tax codes
  with QBOID set, so the "first run on fresh tenant" failure mode
  doesn't reproduce. If it ever does, add the helper and the GET-
  by-QBOID filter, mirroring the firm/employee pattern.

---

## 4. PUT `/TaxCodeEntity/<code>` rejected with `Record not found` after fix #3 due to stale map rows

- **Symptom**: after deploying fix #3 (random 4-char UUID codes),
  the next attempt routed every component through PUT because
  prior attempts had written `map_tax_code` rows for all 9. Of
  those, 5 used the OLD long sanitized codes (from the pre-#3
  attempt that partially succeeded with `_vp_tax_code_value`),
  and 4 used the NEW UUID codes. The 4 UUID PUTs succeeded; the
  5 long-code PUTs all failed with `Failed with error: Record
  not found. Record not found.`:
  ```
  PUT /TaxCodeEntity/California-California                                       → Record not found
  PUT /TaxCodeEntity/CA-Contra_Costa-Contra_Costa-San_Pablo-California_State     → Record not found
  PUT /TaxCodeEntity/FCAC                                                        → OK
  PUT /TaxCodeEntity/D1CA                                                        → OK
  PUT /TaxCodeEntity/1F31                                                        → OK
  PUT /TaxCodeEntity/Out_of_scope-NO_TAX_SALES                                   → Record not found
  PUT /TaxCodeEntity/6062                                                        → OK
  PUT /TaxCodeEntity/Tucson-AZ_State_tax                                         → Record not found
  PUT /TaxCodeEntity/Tucson-Tucson_City                                          → Record not found
  ```
- **Root cause**: when the pre-#3 attempt POSTed with long
  sanitized codes (e.g. `California-California`), VP accepted the
  POST but stored the record under a **truncated** uppercase code
  (probably `CALIFORNIA` — 10-char limit). Our successful POST
  path then wrote the LONG code we sent into `map_tax_code`, so
  subsequent runs try to PUT `/TaxCodeEntity/<long_code>` and VP
  can't find the truncated row by that key. Effectively the local
  map is stale: it points at codes that never existed on the VP
  side. Fix #3 stopped writing new stale codes (UUIDs are inside
  the 10-char limit so they round-trip cleanly), but didn't
  recover the rows that were already poisoned.
- **Fix**: graceful PUT-not-found fallback to POST. In
  `sync_qbo_tax_codes_to_vp`, wrap the PUT in a nested try; if
  the exception message contains `'Record not found'`, log a
  warning and fall through to the existing POST path with a
  fresh UUID code. The bottom `_upsert_map_tax_code_row` then
  rewrites the map row with the fresh code, self-healing. Any
  other PUT exception re-raises (preserves the per-record error
  surface). Same `summary['created']` counter increments — the
  fallback POST is the only POST these records will see this
  run.
- **Out-of-band side effect**: the original truncated VP records
  (e.g. `CALIFORNIA`, `CA-CONTRA_`, `TUCSON-AZ_`, `TUCSON-TUC`,
  `OUT_OF_SCO`) are now orphaned — no `map_tax_code` row points
  to them. They retain their `QBOID` so they're identifiable for
  manual cleanup in the VP UI. Not deleted automatically: the
  integration shouldn't be deleting VP records it didn't create.
  In production this scenario should never occur (fix #3 was
  always in place), so the orphan footprint is a one-time
  artifact of the trial migration.
- **Why not pre-query VP by QBOID** (the
  `_find_vp_firm_by_qbo_id` / `_find_vp_employee_by_qbo_id`
  pattern)? VP's TaxCodeEntity carries only the QBO TaxCode Id
  (`QBOID`) — not the QBO TaxRate Id. A single QBO TaxCode maps
  to N rate components, so a GET by QBOID returns N candidates
  with no field to disambiguate which one is "this" rate
  component. Workato itself relies on the `map_tax_code` lookup
  as the authoritative cross-reference rather than re-querying
  VP. The PUT-fallback approach handles the stale-code edge
  case without needing a disambiguation strategy.
- **Workato reference**: n/a — the recipe assumes the
  map_tax_code lookup is always consistent (Workato controls
  the lookup table directly, no out-of-band truncation
  possible). The fallback handles a failure mode specific to
  our pre-#3 code-derivation bug.
- **Code touchpoints**:
  - `utils/python_callable_method.py:sync_qbo_tax_codes_to_vp` —
    PUT wrapped in nested try; on `'Record not found'` fall
    through to POST with a fresh UUID and `did_put` flag.

### Future-revisit alternatives (intentionally not applied)

- **4a. Pre-query VP for the actual code by QBOID + Rate +
  Description match.** Would let us self-correct the map row's
  `VantagepointCode` to the truncated value VP actually stored,
  PUTting in place rather than re-POSTing. Avoids creating
  orphaned VP records on the stale-data recovery path. Doesn't
  help on the production no-orphan path. Worth doing if more
  out-of-band scenarios surface (e.g. tenant runs a VP-side bulk
  edit that renames codes).
- **4b. One-shot migration: walk map_tax_code, detect rows with
  VantagepointCode > 10 chars (or `!= [A-F0-9]{4}`), POST and
  rewrite them.** Same effect as the current PUT-fallback but
  runs once at task start instead of lazily. Slightly more
  defensive but more code; the lazy fallback handles the
  steady-state cases the same way with no migration code path
  to maintain.

---

## 5. POST `/TaxCodeEntity/` rejected with `already exists` from birthday-paradox UUID collisions

- **Symptom**: rarely, the POST inside `sync_qbo_tax_codes_to_vp`'s
  create branch returns VP's `already exists` error. The
  failing `vp_code` is a fresh 4-hex-char id just emitted by
  `_generate_vp_tax_code()` — i.e. the function happened to roll
  a code that's already taken in the tenant's VP namespace.
- **Root cause**: `_generate_vp_tax_code()` emits a 4-character
  hex id (`[A-F0-9]{4}`) — only ~65k combinations. The 4-char
  width is forced by VP's 10-char `Code` column + the tenant
  convention of prefixing with the `QBOCodeName` slug (commit
  history under fix #3 chose 4 hex chars after analysing the
  truncation cases). With N tax components written into VP, the
  birthday-paradox collision probability is `1 − e^(−N²/2·65536)`:
  100 codes ≈ 7 %, 200 codes ≈ 26 %. Over the lifetime of a
  multi-tenant trial each individual run is unlikely to collide,
  but the steady-state probability across many runs climbs into
  the percent range.
- **Why fix #4's PUT-fallback didn't catch this**: that retry
  triggers on PUT `Record not found` (stale map row), not on POST
  `already exists`. Distinct error families, distinct recovery
  paths.
- **Surfaced by**: review-bot flag during the BatchTaskRunOperator
  wrap rollout. Quote: *"`_generate_vp_tax_code()` returns 4 hex
  chars (16⁴ ≈ 65k combinations). The generator's docstring
  acknowledges 'VP rejects with already exists and the per-record
  retry will pick a new uuid' — but there's no retry loop here."*
  — see code-review comment thread on MAP2-3312.
- **Fix applied**: small retry loop around the POST that
  regenerates `vp_code` via `_generate_vp_tax_code()` and rebuilds
  the create body on each attempt. Capped at 3 attempts (well
  above the practical collision rate even at high tenant
  volume). Non-collision exceptions (`'already exists'` not in
  the error string) re-raise immediately — only the specific
  collision case retries. On exhaustion of the 3 attempts the
  last raised exception surfaces normally and the row lands in
  `summary['errors']` via the surrounding `except Exception` —
  same failure semantics as before the fix, just much rarer to
  hit.
- **Task-id uniqueness**: each retry attempt builds its operator
  with a `_post_tax_{code_id}_{rate_id}_attempt_{N}` task_id so
  the inline `.execute()` re-invocations don't collide on
  Airflow's task_id constraint.
- **Workato reference**: the Workato source recipes don't have
  this problem — they let VP pick the `Code` value on POST
  (recipe sends `Code: blank` and reads VP's response). The
  Airflow port deviates by stamping a UUID-prefix on the body
  (the truncation work in fix #3 chose this shape over the
  recipe's `Code: blank` because it stays stable across PUT
  rewrites). The retry loop here is the cost of carrying that
  client-generated id approach.
- **Code touchpoints**:
  - `utils/python_callable_method.py:sync_qbo_tax_codes_to_vp`
    — POST branch wrapped in a 3-attempt retry loop. Catches
    only `'already exists'`; all other exceptions propagate
    on the first attempt.

### Future-revisit alternatives (intentionally not applied)

- **5a. Widen the UUID to 6 hex chars** (`16⁶ ≈ 16 M`
  combinations). Drops collision probability to negligible at any
  tenant size. Constrained by VP's 10-char `Code` column — would
  leave only 4 chars for the QBOCodeName prefix. Acceptable if
  the prefix is purely cosmetic; needs verification that no
  downstream tooling parses the prefix.
- **5b. Switch to recipe shape: POST with `Code: blank`, then
  read VP's assigned code from the response.** Closest to
  Workato parity. Requires PUT bodies to be rebuilt from VP's
  response shape on the stale-data fallback (fix #4) — manageable
  but more refactor than the retry loop.
- **5c. Pre-load existing VP tax codes at task start and skip
  collisions client-side.** Adds one paginated GET at the top of
  the sync, similar to P2's bulk-fetch pattern. Eliminates the
  retry but pays the GET cost on every run.

---

## 6. Operator-based rebuild + VantagepointCode/row-count parity with Workato

- **Symptom**: the populated `map_tax_code` diverged from the Workato
  `lookup_table_data_014-503-psa-map-tax-code.csv` on two counts:
  - **VantagepointCode** didn't match (airflow emitted RateId-based codes;
    Workato emitted existing VP tax-code numbers, e.g. California → `1`/`2`,
    Tucson City → `3`).
  - **Rows were missing** — airflow had 9 rows, Workato 14. The 5 "missing"
    are fan-out duplicates (California ×2, NO TAX SALES ×2, NO TAX PURCHASE
    ×4) where one QBO rate maps to several existing VP tax codes.
- **Root cause**: the original Python port never implemented the recipe's
  step-17 compile join against existing VP tax codes. The Workato recipe
  matches each QBO rate to **every existing VP TaxCodeEntity by name**
  (`vtc ON tr.RateName = vtc.Description OR tcm.VantagepointCode = vtc.Code`)
  and records **one map row per match**, storing the existing VP `Code`
  (not a RateId/UUID); it only *creates* (Code = RateId/uuid) when there's
  no VP match. The port instead minted codes and emitted one row per rate.
- **Fix (operator-based rebuild)**: re-implemented steps 10-18 as the
  `map_tax_code_dag` collection-operator pipeline (mirroring the abbviemst
  `time_export_child` pattern and the `map_account_code` rebuild):
  - `fetch_vp_tax_codes` (VP GET, #10) → `create_qbo_tax_rates` (#12) /
    `create_vp_tax_codes` (#14) / `create_tax_code_map` (#11/#13) →
    `query_tax_group_ids` (#15) → `query_compiled_tax_codes` (#17) →
    `process_qbo_tax_codes` foreach (#18).
  - `IsTaxGroup` is derived by **rate count** per (CodeID, side), not the
    QBO `TaxGroup` flag (recipe #14/#16 `tgi`).
  - The compile `vtc` join records the existing VP code; the foreach adopts
    it (#22/#34) or creates with Code = RateId/UUID-on-collision when there
    is no match (#24), with the CFG_Region tax-group gate (#26).
- **Interim dedup workaround and its reversal**: a first pass kept Workato's
  raw fan-out, which **exploded to ~72 rows** against the trial VP tenant —
  that tenant was polluted with ~8 duplicate-named VP tax codes per rate
  (created by earlier buggy runs), and the un-keyed write let each run
  multiply them. As a stopgap the compile was changed to `GROUP BY
  (CodeID, RateID)` with a RateId-preferred `COALESCE(...)` pick (one row
  per pair) + a `(QBOCodeID, QBORateID)` unique key. That stopped the
  explosion but **diverged from Workato** (dropped the 5 fan-out rows and
  picked RateId instead of the existing VP code). It was reverted to the
  **full `vtc` fan-out** to restore parity (consistent with how
  `map_account_code` matches existing VP accounts by name), with the unique
  key widened to **`(QBOCodeID, QBORateID, VantagepointCode)`** so the
  fan-out rows coexist *and* re-runs stay idempotent (no cross-run growth).
- **Behaviour note / tenant caveat**: this makes the *logic* match Workato
  (match existing VP by name, multi-row, record the VP code); **exact
  VantagepointCode values are tenant-dependent**. Airflow's VP tenant
  (`vantagepointqe2`) isn't Workato's and is polluted with RateId-coded tax
  codes from earlier runs, so the codes won't byte-match until that tenant
  is cleaned/seeded the same way. Against a clean tenant carrying the same
  VP TaxCodeEntity rows, the fan-out reproduces Workato's 14-row shape
  (verified in a simulation: California → 1,2; NO TAX SALES → 4,5; NO TAX
  PURCHASE → 6,7,8,9).
- **Workato reference**: `014_503_psa_sync_tax_codes.recipe.json` — list VP
  tax codes (#9/#10), compile #16/#17 (`vtc` name-or-mapped-code join),
  foreach #18 (adopt #22/#34, create #24, region gate #26).
- **Code touchpoints**:
  - `utils/_tax_code_sync.py` — staging sources, `TAX_GROUP_IDS_SQL`,
    `COMPILE_TAX_CODES_SQL` (full fan-out), `_read_compiled_tax_codes`,
    `_resolve_create_vp_code`, rewritten `sync_qbo_tax_codes_to_vp`,
    `_upsert_map_tax_code_row` (3-col key).
  - `map_tax_code_dag.py` — the collection-operator pipeline.
  - `common/tables.py` — `MAP_TAX_CODE_UNIQUE_COLUMNS =
    [QBOCodeID, QBORateID, VantagepointCode]`.
  - `utils/python_callable_method.py` — re-exports the staging callables +
    SQL.

### Future-revisit alternatives (intentionally not applied)

- **6a. Keep the dedup (one row per (CodeID, RateID), RateId-preferred).**
  Cleaner data, idempotent, no clean-tenant requirement — but does not
  match Workato's row count or VP codes. Rejected in favour of parity.
- **6b. Collapse the within-run `tcm × vtc` cartesian on re-runs.** On a
  populated map the compile can fan out further before the unique key
  collapses it at write time; harmless (idempotent, and change-detection
  suppresses redundant VP writes) but wasteful. Add a `DISTINCT`/dedupe in
  the compile if re-run cost ever matters.

---

## General notes

- The Workato POST and PUT bodies for VP tax codes are
  intentionally minimal. Sales-vs-Purchase distinction, group
  membership, and the QBO Code+Rate composite key are all carried
  in the **local** `map_tax_code` lookup table only — they don't
  round-trip to VP. Sticking to recipe parity is the safest
  default unless a tenant explicitly needs a different shape.
- The flatten step (`flatten_qbo_tax_rates`) is the source of
  truth for the `(CodeID, RateID)` pair semantics — adjustments
  to the per-row meaning belong there, not in the body builders.
