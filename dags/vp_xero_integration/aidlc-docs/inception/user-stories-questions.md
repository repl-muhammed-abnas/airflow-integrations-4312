# User Stories — Decisions Required

Please answer inline after each `[Answer]:` tag. These decisions finalize the user stories and shape the Airflow design. Recommended option is listed first where applicable. **Q1, Q2, Q3 are blocking** (they change story scope/structure).

---

## Question 1  *(BLOCKING — scope)*
Is the **employee mapping sync** (`map_employee`, Workato `synch_employees`) in scope for this effort, alongside firms / accounts / tax codes?

A) No — firms, accounts, tax codes only (employee deferred to a later effort)
B) Yes — include employee sync (requires a follow-up `synch_employees` recipe analysis before it is development-ready)
C) Other (describe below)

[Answer]: A — No. Employee sync descoped. US-7 marked DESCOPED; MAPPING_STEPS_ORDERED = firm/account/tax (10/20/30).

---

## Question 2  *(BLOCKING — seeding architecture)*
How should the lookup-table **seeding** (placeholder rows) be implemented, given the Workato package splits seeding from sync but the QBO Airflow reference does both in one pass?

A) Merge seeding into each `map_*` engine (QBO parity — recommended; fewer moving parts, no `1900-01-01` sentinel)
B) Mirror Workato — separate `seed_*` task per table, gated by "table empty", then the sync fills it
C) Other (describe below)

[Answer]: A — Yes, merge into the `map_*` engines (QBO parity). Stories US-3/4/5 updated; no separate seed task / sentinel.

---

## Question 3  *(BLOCKING — Xero connectivity)*
What is the **Xero connection/operator** situation in RAIL and the middleware?

A) RAIL already provides Xero operators (contacts, accounts, tax rates) + a Xero connection; conf key `connections.xero`, conn-id `xero_default` — confirm and proceed
B) Some Xero operators are missing — list the gaps to raise with the RAIL team (no custom operators per CLAUDE.md)
C) I don't know — please verify against `replicon-airflow-library` and `airflow-integrations/dags/xero/` and report
D) Other (describe below)

[Answer]: Yes — RAIL has Xero support at `replicon-airflow-library/rail/rail/operators/xero_internal` (generic `XeroAPIOperator` + `XeroHook`). Completed a full Xero API inventory across all of `014-501 PSA` → see `reverse-engineering/xero-mapping-sync/08-xero-api-inventory.md`. Gaps captured as **US-9** (RAIL operator enablement): G1 pagination (HIGH, blocks mapping_sync), G2 PUT (GL only), G3 typed operators (optional), G4 change-feed trigger (polling). For mapping_sync (firm/account/tax = all GET), only **G1 (pagination)** is a hard dependency.

---

## Question 4  *(firm matching strategy)*
Xero contacts are matched to VP firms by **Name** today (no Xero ContactID stored in VP). Keep this, or improve?

A) Keep Workato behaviour — match by Name with `MIN(ClientID)` (fastest parity)
B) Improve — also persist Xero `ContactID` into a VP custom field to enable durable ID-based matching going forward
C) Other (describe below)

[Answer]:

---

## Question 5  *(validation vs self-heal placement)*
Where should the **self-heal / cleanup** behaviours live (account col6/col7 refresh + back-fill; firm archived-contact row deletion), and how should validation failures be signalled?

A) Keep validation read-only/reporting; move self-heal + archived-cleanup into the sync engines; signal failures via `mapping_table_state.Status='Error'` + dispatcher hard-fail (QBO behaviour — recommended)
B) Replicate Workato exactly — self-heal inside the validation recipes; return a JSON error string (note: also fix the firm-validator return bug)
C) Other (describe below)

[Answer]:

---

## Question 6  *(account orphan deactivation)*
The Workato accounts orchestrator sets `Status=I` on **any** VP account not present in the mapping table (including manually-created VP accounts). Keep?

A) Scope it — only deactivate VP accounts that were previously Xero-sourced (avoid deactivating manually-created VP accounts)
B) Keep Workato behaviour — deactivate any unmapped VP account
C) Do not deactivate during initial sync (log only)
D) Other (describe below)

[Answer]:

---

## Question 7  *(map_account_type representation)*
The Xero `Map Account Type` lookup ships with ~16 seed rows (Xero account type → VP type code). How to represent it in Airflow?

A) Seeded S3 collection — data-driven, matches Workato; seeded by `init_mapping_collections` (recommended)
B) Static Python constant in `common/tables.py` (like QBO's `ACCOUNT_TYPE_MAP`)
C) Other (describe below)

[Answer]:

---

## Question 8  *(logging & notifications)*
How should run logging and notifications work (Workato wrote failures to a `014-501 PSA Log` lookup and sent emails; QBO uses RAIL log operators + middleware run-details)?

A) Use RAIL log operators + `PostDagRunDetailsToMiddlewareApiOperator` (QBO parity — recommended); no `log` collection
B) Reproduce the Workato `log` lookup table as a collection
C) Other (describe below)

[Answer]:

---

## Question 9  *(handling identified Workato bugs)*
Several latent bugs were found in the recipes (firm-validator always returns blank; tax-seeder `rows.first`; account-seeder INNER-JOIN drops unmapped types). For the Airflow port:

A) Fix all identified bugs in the port; document each fix in the per-table fix-log (recommended)
B) Replicate Workato behaviour exactly for now (preserve quirks), fix later
C) Other / decide per-bug (describe below)

[Answer]:

---

## Question 10  *(Xero API pagination)*
The Workato recipes do **not** paginate Xero list calls (contacts especially). Confirm the Airflow port must page through all results.

A) Yes — page all Xero list calls (contacts, accounts, tax rates) (recommended)
B) No — single-page is acceptable for our tenant sizes
C) Other (describe below)

[Answer]:
