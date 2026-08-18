# 02 — Synch Accounts (Chart of Accounts, Xero → Vantagepoint)

**Workato recipes:**
- Orchestrator: `…/Mapping/Initial Synch/014_501_psa_synch_accounts.recipe.json`
- Worker (real logic): `…/GL/014_501_psa_sync_accounts.recipe.json` ("014-501 PSA Sync Accounts")

**Direction:** **Xero → Vantagepoint**. Populates `014-501 PSA Map Chart of Accounts`, translating type via `014-501 PSA Map Account Type`.
**Airflow target:** `vp_xero_integration/mapping_sync/map_account_code_dag.py` + `utils/_account_sync.py`

> The Initial Synch `synch_accounts` recipe is an **orchestrator/cleanup** wrapper. The per-account create/map logic lives in the GL worker recipe it calls. Both are documented; the Airflow child DAG must reproduce **both**.

---

## A. Orchestrator — `014-501 PSA Synch Accounts`

### Trigger
`workato_recipe_function / execute`, no params, concurrency 1.

### Phases
1. Read all `Map Chart of Accounts` entries.
2. For each mapped row that has a Xero ID (col7) but **no** VP code (col4 blank) → call worker `014-501 PSA Sync Accounts` with `XeroCode=col1`.
3. Re-read `Map Chart of Accounts` (fresh).
4. Build "Mapped Accounts" collection (col4 = VP Code).
5. List all VP accounts → "Vantagepoint Accounts" collection.
6. SQL anti-join: VP accounts **not** in the mapping table.
7. Deactivate each such VP account (`Status=I`).

### Steps
| # | Op | Logic |
| --- | --- | --- |
| 1 | lookup get_entries `Map Chart of Accounts` | all rows |
| 2 | **foreach** entries | |
| └3 | **if** `present(col7)` AND `blank(col4)` | Xero ID but no VP code → not yet synced |
| &nbsp;&nbsp;└4 | call_recipe `014-501 PSA Sync Accounts` | param `XeroCode=col1` |
| 5 | lookup get_entries (fresh) | re-read |
| 6–7 | declare_list / create_list "Mapped Accounts" | `Code=col4` |
| 8 | VP `chart_of_accounts` **list** | all VP accounts |
| 9 | create_list "Vantagepoint Accounts" | index Account |
| 10 | query_list (anti-join) | `SELECT Account,Name,Type FROM va WHERE NOT EXISTS (SELECT * FROM ma WHERE ma.Code=va.Account)` |
| 11 | **foreach** rows | |
| └12 | VP `chart_of_accounts` **put** | `Status=I`, Detail=1, balancing accts blanked, `QBOAccountID==skip` |

### Notes
- No try/catch in orchestrator (errors surface from the worker, which logs).
- Deactivation is idempotent. ⚠ It will deactivate **any** VP account not in the mapping table — including manually-created VP accounts (see open questions).

---

## B. Worker — `014-501 PSA Sync Accounts` (per-account logic)

### Trigger
`workato_recipe_function / execute`, optional input **`XeroCode`**. Supplied → sync that one account; blank → process all ACTIVE Xero accounts.

### Phases
1. Init `ErrorMessage` / `CompoundError`; open try.
2. Fetch Xero accounts (filtered) + VP accounts (list) + both lookup tables.
3. Build collections (Xero Accounts, VP Accounts, Mapped Accounts, Mapped Account Types).
4. One SQL join (Xero primary) translating Type via Map Account Type.
5. Foreach row: add mapping if missing; create VP account if missing; update VP account if mapped; keep mapping in sync.
6. Catch → `014-501 PSA Log message`.

### Steps (condensed; nesting indented)
| # | Op | Logic |
| --- | --- | --- |
| 1 | declare_variable | `ErrorMessage="Failed to add/update xero account #{XeroCode}…"`, `CompoundError` |
| 2 | **try** | wraps 3–40 |
| 3 | Xero `search_accounts` | `Code=XeroCode.presence ?? skip`; `Status = XeroCode.present? ? skip : "ACTIVE"` |
| 4 | VP `chart_of_accounts` **list** | all VP accounts |
| 5 | lookup get_entries `Map Chart of Accounts` | |
| 6 | lookup get_entries `Map Account Type` | |
| 7–13 | build collections | Xero Accounts (idx AccountID), VP Accounts (idx Account), Mapped Accounts (composite idx), Mapped Account Types |
| 14 | query_list (SQL join) | see below → per-row decision + `MappedVantagepointType` |
| 15 | **foreach** rows | |
| └16 | **if** `blank(EntryID)` AND `XeroStatus=ACTIVE` | new mapping |
| &nbsp;&nbsp;└17 | lookup **add_entry** | col1=XeroCode,col2=XeroName,col3=XeroType,col4=VPCode,col5=VPName,col6=VPType,col7=XeroID |
| └18 | declare `EntryID = row.EntryID.presence ?? add_entry.id` | |
| └19 | **if** `blank(MappedVantagepointCode)` | not yet mapped |
| &nbsp;&nbsp;└20 | **if** `present(VantagepointCode)` | exists in VP by code |
| &nbsp;&nbsp;&nbsp;&nbsp;└21 | **if** code==XeroCode AND name==XeroName | exact match |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└22 | lookup **update_entry** id=EntryID | col4/col5/col6 = VP code/name/type |
| &nbsp;&nbsp;└23 | **if** `blank(VantagepointCode)` AND `XeroStatus=ACTIVE` | must create in VP |
| &nbsp;&nbsp;&nbsp;&nbsp;└24 | **try** | |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└25 | VP `system_formats` | get AccountLength |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└26 | **if** `XeroCode.length > AccountLength` | |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└27 | call `Log message` | "Account number exceeds maximum…" (does NOT map — ⚠ repeats each run) |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└29 (else) | VP `chart_of_accounts` **post** | Account=XeroCode, Name=`XeroName.slice(0,39)`, **Type=MappedVantagepointType**, Status=A, Detail=1, balancing/QBOAccountID blank |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└30–31 catch | `CompoundError +=` "Unable to add account…" | |
| &nbsp;&nbsp;&nbsp;&nbsp;└32 | lookup update_entry id=EntryID | col4=post.Account, col5=post.Name, col6=post.Type |
| └33 | **if** mapped-code present AND `blank(MappedXeroID)` AND VP name==XeroName | backfill Xero side |
| &nbsp;&nbsp;└34 | lookup update_entry id=EntryID | col1=XeroCode,col2=XeroName,col3=XeroType,col7=XeroID |
| └35 | **if** VP code present AND mapped-code present AND MappedXeroID present | confirmed pair → update VP |
| &nbsp;&nbsp;└36 | **try** | |
| &nbsp;&nbsp;&nbsp;&nbsp;└37 | VP `chart_of_accounts` **put** | Account=VPCode, Name=`XeroName.slice(0,39)`, Type=VPType, **Status=`XeroStatus==ACTIVE ? 'A' : 'I'`**, balancing/QBOAccountID blank |
| &nbsp;&nbsp;&nbsp;&nbsp;└38–39 catch | `CompoundError +=` "Unable to update account…" | |
| &nbsp;&nbsp;└40 | lookup update_entry id=EntryID | col2=XeroName, col5=XeroName |
| 41–42 catch | call `Log message` | ErrorMessage + CompoundError + catch.message |

### Step-14 SQL (Xero primary)
```sql
SELECT xa.Code XeroCode, xa.Name XeroName, xa.Type XeroType, xa.AccountID XeroID, xa.Status XeroStatus,
       va.Account VantagepointCode, va.Name VantagepointName, va.Type VantagepointType, va.Status VantagepointStatus,
       ma.XeroID MappedXeroID, ma.VantagepointCode MappedVantagepointCode, ma.EntryID,
       mat.VantagepointType MappedVantagepointType
FROM "Xero Accounts" xa
LEFT JOIN "Vantagepoint Accounts" va ON xa.Code = va.Account
LEFT JOIN "Mapped Accounts" ma ON xa.AccountID = ma.XeroID OR va.Account = ma.VantagepointCode
LEFT JOIN "Mapped Account Types" mat ON UPPER(xa.Type) = UPPER(mat.XeroType)
WHERE xa.Type != 'BANK'
```

## External calls
**Xero:** `search_accounts` (filter Code/Status) → AccountID, Code, Name, Status, Type, TaxType, Class. (Single page in recipe.)
**Vantagepoint:** `chart_of_accounts` **list/post/put**; `system_formats` (AccountLength). Key field `Account`; `Name` truncated to 39; `Type` = mapped VP type; balancing fields + `QBOAccountID` blanked.

## Lookup tables touched
### `014-501 PSA Map Chart of Accounts`
| col | label |
| --- | --- |
| col1 | Xero Code |
| col2 | Xero Name |
| col3 | Xero Type |
| col4 | Vantagepoint Code |
| col5 | Vantagepoint Name |
| col6 | Vantagepoint Type |
| col7 | Xero ID |
| col8 | Messages |

- **Upsert key = lookup `id` (EntryID)** (from query row or `add_entry.id`).
- **Match precedence:** Xero `AccountID` ↔ col7, OR VP `Account` ↔ col4, OR Xero `Code` ↔ VP `Account`.

### `014-501 PSA Map Account Type` (translation table — has seed data, ~16 rows)
Columns used: col3 = Xero Type, col4 = Vantagepoint Code (the VP **Type** value).
**Translation:** SQL `UPPER(xa.Type)=UPPER(mat.col3)` → read col4 as `MappedVantagepointType`, written as VP account `Type` on create. **No fan-out** (1 Xero type → 1 VP type). BANK accounts excluded.

## Matching / dedup & direction
Xero→VP. EntryID present = mapping exists; MappedXeroID present = fully linked. Re-runnable: existing mappings hit PUT, new ones hit POST.

## Error handling / logging
Inner try/catch around create (24–31) and update (36–39) append to `CompoundError` (no throw). Length violation logs immediately. Outer catch → `014-501 PSA Log message`. Map table has a `Messages` (col8) column but the worker doesn't write it here (the tax recipe does write Messages).

## Idempotency / re-run
Existing maps → update path; new maps inserted once by EntryID; Name re-truncated + Status re-synced each run.

---

## Airflow design notes (→ `map_account_code_dag.py` / `_account_sync.py`)
- **Mirror QBO `_account_sync.py`**: stage `xero_accounts`, `vp_accounts`, `account_code_map` collections; one `COMPILE_ACCOUNT_CODES_SQL` (the step-14 join above) producing the per-row decision; iterate to add-mapping / create-VP / update-VP.
- **`ACCOUNT_TYPE_MAP`:** QBO uses a **static Python constant** keyed on QBO `Classification`. Xero instead has a **populated lookup table** `Map Account Type` (col3 Xero Type → col4 VP Code). Decide: keep it as an S3 collection (data-driven, matches Workato) OR port to a static constant in `common/tables.py`. **Recommendation:** keep as a seeded collection since the Xero file already ships data — see [04-lookup-tables.md](04-lookup-tables.md).
- **Account length guard:** reproduce `system_formats` AccountLength check; decide whether to keep Workato's behaviour (log + skip mapping, repeats each run) or improve (write `Messages` and mark error once).
- **Orchestrator cleanup (deactivate orphans):** reproduce the anti-join deactivation as a final task — but gate it (see open question on deactivating manually-created VP accounts).
- **VP ops unchanged** (`VantagepointChartOfAccountsOperator`, `VantagepointSystemFormatsOperator`).
- **Source op:** `rail.XeroAccountOperator` `search_account` equivalent (filter Code/Status).
