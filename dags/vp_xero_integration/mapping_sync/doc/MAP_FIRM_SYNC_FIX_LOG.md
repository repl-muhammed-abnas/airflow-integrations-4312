# MAP_FIRM_SYNC_FIX_LOG

Per-table log for `map_firm_dag.py` + `utils/_firm_sync.py` (Xero Contacts → VP
Firms). Records where the Airflow port **deliberately diverges from the Workato
recipes** (`014_501_psa_synch_firms` + the `Map Firms` seeder) to fix a Workato
bug or limitation — decision **Q9** ("fix all identified bugs in the port").
Each entry: **Symptom → Root cause → Fix → Workato reference → Code touchpoints**.

Parity spec: `aidlc-docs/.../xero-mapping-sync/01-synch-firms.md` +
`06-lookup-table-seeding.md`.

---

## #1 — Vendor/Client codes left blank by the seeder

**Symptom.** In Workato the `Map Firms` seeder writes placeholder rows with
col4 (Vendor) / col5 (Client) **blank**; nothing ever derives them, so the
firm map never records which side of the relationship a contact is.

**Root cause.** The seeder has no `IsCustomer`/`IsSupplier` filter and doesn't
parse the Xero `AccountNumber`; it just stamps ContactID/Name placeholders.

**Fix.** The merged engine derives both during the sync:
- `ClientInd`/`VendorInd` on the VP firm from the Xero contact's
  `IsCustomer`/`IsSupplier` (a contact can be both).
- `Vendor`/`Client` numeric codes parsed from the Xero `AccountNumber` with the
  `SL<client>/PL<vendor>` convention (`_parse_account_number`), written into the
  `map_firm` row (col4/col5).

**Workato reference.** `synch_firms` step 16/21 (AccountNumber parse);
`06-lookup-table-seeding.md` §1 "No IsCustomer/IsSupplier filter … col4/col5
left blank".

**Code touchpoints.** `_firm_sync._parse_account_number`,
`build_vp_firm_create_body`, `_build_map_firm_row`.

---

## #2 — `/Contacts` not paginated → large tenants under-seed

**Symptom.** Tenants with more than one page of contacts (~100) only get the
first page mapped; the rest are silently missing.

**Root cause.** The Workato adhoc `GET /Contacts` issues a single request with
no page loop.

**Fix.** The fetch uses `XeroContactOperator(operation='search',
include_archived=True, paginate=True)` — RAIL pagination (delivered in U0/G1)
loops every page. `include_archived=True` is required so the archived-contact
checks/cleanup see the full set.

**Workato reference.** `06-lookup-table-seeding.md` §1 "No pagination on
`GET /Contacts` → large tenants under-seed"; `08-xero-api-inventory.md` G1.

**Code touchpoints.** `map_firm_dag.fetch_xero_contacts`.

---

## #3 — Address creation has no dedup → duplicate addresses on re-run

**Symptom.** Re-running the recipe against a re-created firm stacks duplicate
STREET/POBOX addresses.

**Root cause.** Workato assigns a fresh `CLAddressID` uuid on every run with no
"already created this address" guard.

**Fix.** The engine dedups within a run by `(ClientID, AddressType, Address1)`
before POSTing each address; an address already seen is skipped. (A still-fresh
`CLAddressID` is generated per new address, matching VP's expectation.)

**Workato reference.** `01-synch-firms.md` §8 "Address creation has no dedup".

**Code touchpoints.** `_firm_sync.sync_xero_firms_to_vp._create_firm_addresses`
(the `seen_addresses` set), `build_vp_firm_address_bodies`.

---

## #4 — Match-by-name reuses MIN(ClientID); no duplicate firms

**Symptom (parity, not a bug).** A Xero contact whose Name already exists in VP
must reuse the existing firm, not create a second one.

**Fix.** `_load_vp_firms_by_name` indexes VP firms by Name keeping the
lexicographically smallest ClientID (`MIN(ClientID)` collapse, Workato step 13,
decision **Q4 = A**); the engine reuses it and skips creation. Net-new contacts
(no name match) create the VP firm + addresses.

**Workato reference.** `synch_firms` step 13/17.

**Code touchpoints.** `_firm_sync._load_vp_firms_by_name`, the match branch in
`sync_xero_firms_to_vp._process_one`.

---

## Known follow-up (not yet implemented)

- **Archived-contact cleanup.** Workato's firm validator deletes `map_firm` rows
  whose ContactID is an ARCHIVED Xero contact. Per **Q5**, that cleanup belongs
  in the firm sync engine (validation stays read-only). The engine fetches
  archived contacts but does not yet delete archived-mapped rows — the validator
  currently skips ARCHIVED rows rather than flagging them. Track as a follow-up.
