# MAP_ACCOUNT_CODE_SYNC_FIX_LOG

Per-table log for `map_account_code_dag.py` + `utils/_account_sync.py` (Xero
Chart of Accounts → VP Chart of Accounts, written into `map_chart_of_accounts`).
Records deliberate divergences from the Workato recipes
(`014_501_psa_synch_accounts` orchestrator + `014_501_psa_sync_accounts` worker +
the `Map Accounts` seeder) per decision **Q9**. Each entry: **Symptom → Root
cause → Fix → Workato reference → Code touchpoints**.

Parity spec: `aidlc-docs/.../xero-mapping-sync/02-synch-accounts.md` +
`06-lookup-table-seeding.md`.

---

## #1 — Unmapped Xero account types silently dropped (INNER JOIN)

**Symptom.** Xero accounts whose `Type` has no row in `Map Account Type` never
appear in the mapping — they vanish with no record or warning (potential data
loss).

**Root cause.** The Workato `Map Accounts` seeder uses an **INNER JOIN** to
`Map Account Type` (`xa.Type = at.Type`), so unmapped types are filtered out
entirely.

**Fix (Q9 / Q-S3).** The type translation is resolved in Python from the seeded
`map_account_type` collection (`_load_account_type_index`), **not** an INNER
JOIN. When a Xero type has no mapping the account is **surfaced**: a
`map_chart_of_accounts` row is still written with a `Messages` note
(`"No VP type mapping for Xero type '<X>'; account not created."`) and the
`unmapped_type` counter increments — nothing is dropped.

**Workato reference.** `06-lookup-table-seeding.md` §2 "INNER JOIN to
Map Account Type → silently dropped".

**Code touchpoints.** `_account_sync._load_account_type_index`,
`COMPILE_ACCOUNT_CODES_SQL` (LEFT-join shape; type resolved in Python),
the `type_code is None` branch in `sync_xero_accounts_to_vp`.

---

## #2 — `map_account_type` kept as a seeded collection, not a static constant

**Symptom (decision, not a bug).** The QBO port hardcodes its account-type map
as a Python dict; the Xero lookup table ships real `data`.

**Fix (Q7 = A).** Kept data-driven: `init_mapping_collections` seeds
`map_account_type` (16 rows, `ACCOUNT_TYPE_SEED_ROWS`) and the engine reads it at
runtime. Changing the mapping is a data edit, not a code change.

**Workato reference.** `02-synch-accounts.md` Airflow notes; `04-lookup-tables.md`
Q-A1.

**Code touchpoints.** `common/tables.ACCOUNT_TYPE_SEED_ROWS`, dispatcher
`init_mapping_collections`, `_account_sync._load_account_type_index`.

---

## #3 — Orphan deactivation scoped to Xero-sourced accounts

**Symptom.** The Workato orchestrator's anti-join deactivates **any** VP account
not in the mapping table — including manually-created VP accounts the
integration never owned.

**Root cause.** The cleanup anti-join has no "was this Xero-sourced?" predicate.

**Fix (Q6 = A).** The orphan pass deactivates **only** VP accounts that are
previously Xero-sourced — i.e. present in `map_chart_of_accounts` with a
`VantagepointCode` — whose `XeroID` is no longer in the current Xero account set.
Manually-created VP accounts (never in the map) are never touched. Each
deactivation PUTs `Status='I'` and records a `Messages` note on the map row.

**Workato reference.** `02-synch-accounts.md` §A "deactivate any VP account not
in the mapping table — including manually-created".

**Code touchpoints.** the Phase-1b orphan loop in `sync_xero_accounts_to_vp`,
`_read_existing_map_rows`.

---

## #4 — Account-length guard surfaces instead of looping a log forever

**Symptom (parity hardening).** Workato logs "Account number exceeds maximum"
on every run for an over-length code and never maps it (repeats indefinitely).

**Fix.** `_resolve_vp_account_max_len` reads the tenant `AccountLength` from VP
System Formats once; an over-length Xero code is recorded once with a `Messages`
note (`skipped_account_code_too_long`) instead of attempting the create.

**Workato reference.** `02-synch-accounts.md` worker step 26/27.

**Code touchpoints.** `_account_sync._resolve_vp_account_max_len`,
`map_account_code_dag.get_system_formats`, the length-guard branch in the engine.

---

## #5 — Idempotent upsert keyed on XeroID

**Symptom (parity, not a bug).** Re-runs must converge, not stack rows.

**Fix.** `map_chart_of_accounts` declares a UNIQUE index on `XeroID`; the engine
accumulates rows and writes them in one batched `S3UpsertCollectionOperator`
keyed on `XeroID` (rows with a blank XeroID are skipped). Existing mappings hit
the PUT/rate path; new ones hit POST.

**Workato reference.** `02-synch-accounts.md` §"Idempotency / re-run".

**Code touchpoints.** `common/tables.MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS`,
the Phase-2 upsert in `sync_xero_accounts_to_vp`.
