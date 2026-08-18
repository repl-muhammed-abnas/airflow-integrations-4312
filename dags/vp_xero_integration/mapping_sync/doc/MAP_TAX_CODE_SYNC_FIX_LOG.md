# MAP_TAX_CODE_SYNC_FIX_LOG

Per-table log for `map_tax_code_dag.py` + `utils/_tax_code_sync.py` (Xero
TaxRates → VP Tax Codes). Records deliberate divergences from the Workato
recipes (`014_501_psa_sync_tax_codes` GL worker + the `Map Tax Codes` seeder)
per decision **Q9**. Each entry: **Symptom → Root cause → Fix → Workato
reference → Code touchpoints**.

Parity spec: `aidlc-docs/.../xero-mapping-sync/03-sync-tax-codes.md` +
`06-lookup-table-seeding.md`.

> Defining shape: Xero models tax as **TaxRates with nested TaxComponents[]**.
> The engine flattens each ACTIVE rate into one row per component (fan-out — one
> Xero rate → several VP tax codes) and links **compound** components to their
> base component's VP code.

---

## #1 — Compile-join OR-precedence bug

**Symptom.** The step-16 join's `vtc` (Vantagepoint Tax Codes) match returns the
wrong rows: the unparenthesized `A AND B OR C` binds as `(A AND B) OR C`, so the
mapped-code branch (`tcm.VantagepointCode = vtc.Code`) matches independently of
the rate/component, cross-joining unrelated VP codes.

**Root cause.** Workato's SQL omits parentheses around the two OR'd join
conditions.

**Fix (Q-T1).** `COMPILE_TAX_CODES_SQL` parenthesizes explicitly:
`ON (xtc.RateName = vtc.Description AND xtc.ComponentName = vtc.Code) OR
(tcm.VantagepointCode = vtc.Code)`.

**Workato reference.** `03-sync-tax-codes.md` step-16 SQL + §"Step-16 join
precedence bug".

**Code touchpoints.** `_tax_code_sync.COMPILE_TAX_CODES_SQL`. Asserted by
`_tax_code_sync_test.test_compile_sql_parenthesizes_or_join`.

---

## #2 — Tax seeder broadcast `rows.first` instead of iterating per row

**Symptom.** The Workato `Map Tax Codes` seeder's `add_batch_of_entries` column
params reference `rows.first.XeroRateName` etc. (the **first** result row) while
the source is the full `rows` array — so every seeded row gets the first row's
values.

**Root cause.** Datapill bug: `.first` used where `current_item` was intended
(the firm/account seeders use `current_item` correctly).

**Fix (Q9 / Q-S4).** The Airflow port iterates **per compiled row** in
`sync_xero_tax_codes_to_vp` and writes each row's own (RateName, ComponentName,
Rate, …) values — no `.first` broadcast.

**Workato reference.** `06-lookup-table-seeding.md` §3 "Likely datapill bug".

**Code touchpoints.** the per-row Pass-1 loop in `sync_xero_tax_codes_to_vp`.

---

## #3 — Component fan-out + compound linking (two-pass)

**Symptom (load-bearing logic, not a bug).** A compound TaxComponent must
reference its base component's VP code via VP `CompoundOnTaxCode`, but the base
VP code only exists after the base component is created.

**Fix.** Reproduced as a two-pass engine:
- `flatten_xero_tax_rates` emits one row per ACTIVE rate × component (`IsCompound`
  stored as `'t'/'f'`; `ReportTaxType` defaults to `'none'`).
- The compile orders by `IsCompound` so the non-compound base is processed first;
  Pass 1 creates/reuses each component's VP code and records
  `(RateName, ComponentName) → VP code`.
- Pass 2 resolves each compound component's base VP code (from the
  `RateName#ComponentName` subquery reference) and PUTs `CompoundOnTaxCode`,
  patching the map row's `CompoundOnCode` (col5).
- The map UNIQUE key is `(XeroName, XeroCode)` = (RateName, ComponentName), so
  the fan-out rows coexist and re-runs converge.

**Workato reference.** `03-sync-tax-codes.md` steps 16–40, §"Compound linking".

**Code touchpoints.** `_tax_code_sync.flatten_xero_tax_rates`,
`COMPILE_TAX_CODES_SQL` (compound subquery), the `compound_links` accumulator +
Pass-2 loop in `sync_xero_tax_codes_to_vp`.

---

## #4 — VP-code generation + Sequence high-water-mark

**Symptom (parity).** Net-new components need a stable generated VP code that
doesn't collide on re-run.

**Fix.** `_generate_vp_code(seq)` produces `'X' + seq.rjust(4,'0')` (e.g.
`X0007`). The sequence counter is initialised from the max existing
`Sequence` in the map (`_max_existing_sequence`), so re-runs never reissue a
code. Existing components reuse their stored VP code (no regeneration).

**Workato reference.** `03-sync-tax-codes.md` steps 12/22/23; note the 9999 cap.

**Code touchpoints.** `_tax_code_sync._generate_vp_code`,
`_max_existing_sequence`, the create branch in Pass 1.

---

## #5 — Per-row VP-create errors recorded, loop continues

**Symptom (parity).** A single VP create failure shouldn't abort the whole tax
sync.

**Fix.** A failed VP `POST` records the error in the map row's `Messages` (col7)
and appends to the summary errors without aborting the loop (Workato's
`CompoundError` accumulation). After all rows, a non-empty error list raises a
`RuntimeError` so the dispatcher's catch fires — successful rows are already
upserted.

**Workato reference.** `03-sync-tax-codes.md` steps 29–31, §"Error handling".

**Code touchpoints.** the inner try/except in Pass 1 + the final raise in
`sync_xero_tax_codes_to_vp`.
