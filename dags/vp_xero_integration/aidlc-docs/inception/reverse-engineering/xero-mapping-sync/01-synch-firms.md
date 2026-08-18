# 01 — Synch Firms (Xero → Vantagepoint)

**Workato recipe:** `integration_vantagepoint_xero/code/014-501 PSA/Mapping/Initial Synch/014_501_psa_synch_firms.recipe.json`
**Name / version:** "014-501 PSA Synch Firms" · v131 · concurrency 1
**Stated purpose:** "Create missing Contact records from Xero in Vantagepoint as Firms"
**Direction:** **Xero → Vantagepoint** (reads Xero Contacts, creates VP Firms + addresses, populates `014-501 PSA Map Firm`). No write-back to Xero in this recipe.
**Airflow target:** `vp_xero_integration/mapping_sync/map_firm_dag.py` + `utils/_firm_sync.py`

---

## 1. Trigger
- `workato_recipe_function / execute` — callable recipe **with no input params**. All data gathered internally.
- *Airflow:* child DAG triggered by dispatcher with standard conf (connections, customerId, company_key, CFG block). No per-record conf.

## 2. High-level phases
1. **Init** — declare `ErrorMessage` (seed `"Error Upserting Firms in Vantagepoint\n"`), `ErrorsPresent=false`.
2. **Extract Xero** — GET *all* contacts (active + archived) via custom HTTP; flatten addresses/phones; load into SQLite collection keyed by `ContactID`.
3. **Extract VP firms** — VP firm search; load collection indexed by `ClientID` + `Name`.
4. **Extract mapping state** — read all `014-501 PSA Map Firm` entries; load "Mapped Firms" collection keyed by `ContactID`.
5. **Get VP org** — fetch organizations (default Org for new firms).
6. **Reconcile existing-by-name** — placeholder rows (blank FirmID) whose `Name` already matches a VP firm → delete + re-add mapping with the matched `ClientID` (no duplicate firm).
7. **Identify net-new** — placeholder rows with no VP name match = firms to create; **graceful stop** if none.
8. **Create firms** — batch-create in VP; capture failures.
9. **Build + create addresses** — join Xero address data to new ClientIDs; resolve country/state codes; batch-create STREET ∪ POBOX addresses.
10. **Finalize mapping** — re-query new firms with new ClientID; delete placeholder rows; re-add fully-populated mapping rows.
11. **Logging / notify** — write failed records to `014-501 PSA Log`; call log/notification recipes.
12. **Outer catch** — any unhandled error → `014-501 PSA Log message`.

## 3. Step-by-step (ordered; indentation = nesting)

| # | Operation | Description | Key inputs → outputs / logic |
| --- | --- | --- | --- |
| 1 | declare_variable | run vars | `ErrorMessage`, `ErrorsPresent=false` |
| 2 | **try** | wrap all work | rescued by step 58 |
| 3 | xero `__adhoc_http_action` | **GET all contacts** | `GET api.xro/2.0/Contacts` (custom; default endpoint returns Active only) → `Contacts[]` |
| 4 | declare_list | flatten contacts | per contact: STREET addr (AddressType=="STREET") fields, POBOX fields, `PhoneNumber` from PhoneType DDI else DEFAULT |
| 5 | create_list | "Xero Contacts Collection" | primary index `ContactID`, `force=true` |
| 6 | vantagepoint `firm` search | all VP firms | → `firms[]` (ClientID, Name, ClientInd, VendorInd, …) |
| 7–8 | declare_list / create_list | "Vantagepoint Firms Collection" | index `ClientID` + `Name` |
| 9 | lookup `get_entries` | read `Map Firm` | col1 FirmID, col2 ContactID, col3 Status, col4 Vendor, col5 Client, col6 Xero Name, col7 VP Name, col8 Mod Date |
| 10–11 | declare_list / create_list | "Mapped Firms Collection" | `EntryID=id`, index `ContactID` |
| 12 | vantagepoint `organization` | VP orgs | first Org = default |
| 13 | query_list (SQL) | **matched-by-name** | INNER JOIN mapped(FirmID='') × xero × vp ON `xc.Name=vf.Name`; `MIN(vf.ClientID)` |
| 14 | **if** rows>0 | reconcile existing | |
| └15 | lookup `delete_entries` | delete placeholders | by `EntryID` |
| └16 | lookup `add_batch_of_entries` | re-add with matched ClientID | col1=ClientID, col2=ContactID, col3=ContactStatus, col4/col5 from AccountNumber, col6/col7=Name, col8=now |
| 17 | query_list (SQL) | **net-new** | LEFT JOIN vp ON Name; `WHERE IFNULL(vf.ClientID,'')=''` |
| 18 | **if** rows==0 | none to create | |
| └19 | **stop** (no error) | graceful end | |
| 20 | **try** | wrap firm create | catch=24 |
| └21 | vantagepoint `firm_batch` post | **create firms** | Name, ClientInd=IsCustomer, VendorInd=IsSupplier, Client/Vendor from AccountNumber, Status=A, Org=first, ReadyForApproval=true, AvailableForCRM=N, ReadyForProcessing=N, SortName=Name → `records_ingested[]`, `records_failed[]` |
| └22–23 | if records_failed≠0 | flag | `ErrorsPresent=true` |
| └24–25 | **catch** | flag | `ErrorsPresent=true` |
| 26–27 | declare_list / create_list | "Ingested Records Collection" | index `Name` (link new ClientID by name) |
| 28–29 | vantagepoint `codetable_records` `FW_CFGCountry` | VP countries | index by Description |
| 30–31 | vantagepoint `codetable_records` `CFGStates` | VP states | index by Description |
| 32 | query_list (SQL) | **addresses STREET ∪ POBOX** | join new ClientID by Name; resolve Country/State by Description; tag AddressType |
| 33 | **try** | wrap address create | catch=37 |
| └34 | vantagepoint `firm_address_batch` post | **create addresses** | ClientID, CLAddressID=uuid, PrimaryInd (STREET), Billing/Accounting (POBOX), Address1-4/City/Zip switch on type, State=StateCode, Country=CountryCode, Email, TaxRegistrationNumber, Phone |
| └35–38 | if failed / catch | flag | `ErrorsPresent=true` |
| 39 | query_list (SQL) | **re-query for mapping** | new firms with `irc.ClientID` |
| 40 | **if** rows>0 | finalize mapping | |
| └41 | lookup `delete_entries` | delete placeholders | by EntryID |
| └42 | lookup `add_batch_of_entries` | re-add with new ClientID | same col mapping as step 16 |
| 43 | **if** ErrorsPresent | error path | |
| └44–53 | append errors + write `014-501 PSA Log` | per failed record, col4="Error" | |
| └54 | call `014-501 PSA Log message` | log completion w/ errors | |
| 55 | **else** | success path | |
| └56–57 | start + call `Send Error Notification Email` | "sync completed" email | |
| 58 | **catch** (outer) | unhandled error | |
| └59 | call `014-501 PSA Log message` | ErrorMessage + VP catch.message | |

## 4. External calls
**Xero** (`xero` connector):
- `GET api.xro/2.0/Contacts` (custom adhoc) → `ContactID, Name, AccountNumber, ContactStatus, FirstName, LastName, EmailAddress, TaxNumber, IsCustomer, IsSupplier, UpdatedDateUTC, Addresses[](AddressType, AddressLine1-4, City, Region, PostalCode, Country), Phones[](PhoneType, PhoneNumber, PhoneAreaCode, PhoneCountryCode)`. **No pagination** in the recipe (⚠ see open questions).

**Vantagepoint** (`deltek_vantagepoint_connector_...`):
- `firm` search (all firms).
- `organization` (default org).
- `firm_batch` post → create firms (`records_ingested`/`records_failed`).
- `codetable_records` `FW_CFGCountry`, `CFGStates`.
- `firm_address_batch` post → create addresses.

VP create uses connector **batch** actions (connector iterates the source list internally — not Workato `foreach`).

## 5. Lookup tables touched
### `014-501 PSA Map Firm`
| col | label | meaning |
| --- | --- | --- |
| col1 | Firm ID | VP ClientID |
| col2 | Contact ID | Xero ContactID |
| col3 | Status | Xero ContactStatus |
| col4 | Vendor | Vendor code (from AccountNumber) |
| col5 | Client | Client code (from AccountNumber) |
| col6 | Xero Name | |
| col7 | Vantagepoint Name | |
| col8 | Mod Date | now |

- **Eligibility filter:** only rows with **blank FirmID** (`mf.FirmID=''`) are processed — placeholders seeded at deployment.
- **Functional unique key:** `ContactID` (col2). Workato upserts by delete-`EntryID`-then-add.
- **AccountNumber parsing:** Client (col5) = `AccountNumber.split("/")[0]` minus `"SL"`; Vendor (col4) = `split("/")[1]` minus `"PL"` → convention `SL<client>/PL<vendor>`.

### `014-501 PSA Log`
Written only on partial failure (steps 48, 53): col1=JSON job payload, col2=now, col3=recipe name, **col4="Error"**, col5=`error: record`, col7=job id.

## 6. Matching / dedup logic
- **Match key = firm `Name`** (`xc.Name = vf.Name`). Xero `ContactID` is **not** stored in VP — the mapping table is the only ID bridge.
- Already-exists-in-VP → reuse `MIN(ClientID)`, no create. No-match → create.
- Vendor vs Client from `IsSupplier` / `IsCustomer` (a contact can be both); numeric codes from `AccountNumber`.
- **One-directional** (Xero→VP). The VP→Xero half is elsewhere.

## 7. Error handling / logging
- `ErrorsPresent` flag + accumulating `ErrorMessage`. Three capture points: partial batch failures, inner catches (firm/address create), outer catch. Failed records → `014-501 PSA Log` with Status `"Error"`. Completion always notifies (error log message or success email).

## 8. Idempotency / re-run
- Self-limiting: only blank-FirmID rows processed; once populated they're skipped. Name-match guard prevents duplicate firms. Mapping writes are delete-then-add (no dup rows).
- ⚠ **Address creation has no dedup** (`CLAddressID` is a fresh uuid each run) — a recreated firm could get duplicate addresses.

---

## 9. Airflow design notes (parity port → `map_firm_dag.py` / `_firm_sync.py`)
- **Source op:** replace Xero adhoc GET with `rail.XeroContactOperator` (or RAIL Xero contacts op) — **must page** through all contacts (active + archived). Confirm operator supports `includeArchived`/paging; if not, raise to RAIL.
- **Collections:** load Xero contacts, VP firms, existing `map_firm` into run-local SQLite collections (`CreateCollectionOperator`); reproduce the three `query_list` SQLs (matched-by-name, net-new, address UNION) as `QueryCollectionOperator` queries.
- **VP target ops:** reuse `VantagepointFirmOperator` (search + batch create), org + codetable custom ops, firm address batch. VP side is unchanged from QBO.
- **`map_firm` schema differs from QBO:** the Xero `map_firm` has 8 columns (FirmID, ContactID, Status, Vendor, Client, Xero Name, VP Name, Mod Date) vs QBO's 4 (FirmID, QBOID, IsVendor, Name). Define the Xero columns in `common/tables.py` — see [04-lookup-tables.md](04-lookup-tables.md). **Natural key = ContactID.**
- **Matching:** QBO matched by `QBOID`; **Xero matches by Name** (no stored id). Reproduce `MIN(ClientID)` collapse and the placeholder-seeding dependency.
- **Idempotency fix opportunity:** add an address dedup key (e.g. ClientID + AddressType + Address1) so re-runs don't duplicate addresses — note as an improvement over Workato.
- **Open dependency:** the placeholder-seeding process (who writes blank-FirmID rows with ContactID) must be defined — likely a `populate_mapping_table` recipe analogue (`Mapping/Lookup Tables/014_501_psa_map_firms.recipe.json`). Flag for the User Story.
