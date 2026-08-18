# 06 — Lookup-Table Seeding (`Mapping/Lookup Tables/`)

**Workato recipes:** `…/014-501 PSA/Mapping/Lookup Tables/`
- `014_501_psa_map_firms.recipe.json` → seeds `014-501 PSA Map Firm`
- `014_501_psa_map_accounts.recipe.json` → seeds `014-501 PSA Map Chart of Accounts`
- `014_501_psa_map_tax_codes.recipe.json` → seeds `014-501 PSA Map Tax Code`

**Direction:** Xero → Vantagepoint. **These answer open question [Q-F3](05-open-questions.md):** they are the **placeholder-seeding** step the Initial Sync depends on.

> **Common skeleton (all three):**
> `lookup_table.get_entries` → **`if entries.size != 0: stop` (idempotency guard)** → pull Xero (+ VP) source data → build in-memory `workato_smart_list` collections → SQL **anti-join** (Xero records *not already mapped*) → **single `add_batch_of_entries`** writing **placeholder rows with the target-system key BLANK**.
> They are **one-time seeders**: once the table has any row, re-runs stop immediately. **None truncates or deletes.** None paginate the Xero calls.

---

## 1. `Map Firms` → `014-501 PSA Map Firm`

| Aspect | Detail |
| --- | --- |
| Trigger | callable, no params, concurrency 1 |
| Guard | `if get_entries.size != 0 → stop` |
| Source calls | **Xero only**: `GET api.xro/2.0/Contacts` (ad-hoc). **No VP call.** |
| Anti-join | `SELECT xc.ContactID, xc.Name, xc.ContactStatus FROM [Xero Contacts] xc WHERE NOT EXISTS (SELECT * FROM [Mapped Firms] mf WHERE mf.ContactID = xc.ContactID)` |
| Written cols | col2=ContactID, col3=Status, col6=Xero Name, **col8=Mod Date hardcoded `1900-01-01T00:00:00.000`** |
| Left BLANK | **col1 Firm ID** (placeholder), col4 Vendor, col5 Client, col7 VP Name |
| Match key | **ContactID** (col2) |

**Consumed by** `synch_firms` ([01](01-synch-firms.md)): it processes only blank-FirmID rows, matches/creates the VP firm, and fills col1 (+ col4/col5/col7, col8). The `1900-01-01` sentinel in col8 means "never synced."

⚠ **Notable behaviours / risks:**
- **No `IsCustomer`/`IsSupplier` filter** — *every* active Xero contact becomes a placeholder firm; col4/col5 (Vendor/Client) are left blank rather than derived here.
- **No pagination** on `GET /Contacts` (Xero pages at ~100) → large tenants under-seed.
- Partial table is never "topped up" (guard stops first); re-seed requires manual clear.

---

## 2. `Map Accounts` → `014-501 PSA Map Chart of Accounts`

| Aspect | Detail |
| --- | --- |
| Trigger | callable, no params, concurrency 1 (`version_comment: "Step 17 — query changed to pick up active Xero accounts only"`) |
| Guard | `if get_entries.size != 0 → stop` |
| Source calls | **Xero** `list_accounts` + **VP** `chart_of_accounts` (verb=list) + lookup **`014-501 PSA Map Account Type`** |
| Join/anti-join | see SQL below |
| Written cols | col1=Xero Code, col2=Xero Name, col3=Xero Type, col4=VP Code, col5=VP Name, col6=`VantagepointType.presence \|\| MappedVantagepointType.presence \|\| skip`, col7=Xero ID |
| Left BLANK | col4/col5 (VP Code/Name) **unless the VP match succeeds** → placeholder |
| Match key | **Xero ID** (col7 == `AccountID`) |

```sql
SELECT xa.AccountID [XeroID], xa.Code [XeroCode], xa.Name [XeroName], xa.Type [XeroType],
       va.Account [VantagepointCode], va.Name [VantagepointName], va.Type [VantagepointType],
       at.VantagepointCode [MappedVantagepointType]
FROM [Xero Accounts] xa
INNER JOIN [Account Type] at ON xa.Type = at.Type                 -- ⚠ INNER: drops unmapped types
LEFT  JOIN [Vantagepoint Accounts] va ON xa.Code = va.Account AND xa.Name = va.Name
WHERE NOT EXISTS (SELECT * FROM [Mapped Accounts] ma WHERE ma.XeroID = xa.AccountID)
  AND xa.Status = 'ACTIVE'
```

**Consumed by** `synch_accounts` ([02](02-synch-accounts.md)): rows with blank col4/col5 are placeholders the worker fills by creating the VP account; col6 already carries the best-guess VP type so creation uses the correct type.

⚠ **Notable behaviours / risks:**
- **INNER JOIN to `Map Account Type`** → Xero accounts whose `Type` has no row in the account-type table are **silently dropped** (never seeded). Confirm intended (vs. data loss).
- **VP pre-fill requires exact `Code` AND `Name` match** — minor name diffs leave col4/col5 blank (→ "needs creation").
- No pagination; both calls are bulk "list all."

---

## 3. `Map Tax Codes` → `014-501 PSA Map Tax Code`

| Aspect | Detail |
| --- | --- |
| Trigger | callable, no params, concurrency 1 |
| Guard | `if get_entries.first.size != 0 → stop` |
| Source calls | **Xero** `list_tax_rates` (nested `TaxComponents[]`) + **VP** `tax_codes` (verb=list) |
| Pre-processing | **`js_eval FlattenTaxRates`** → one row per ACTIVE rate × component; computes `isNewOrUpdated` (unused for gating here) |
| Anti-join | see SQL below |
| Written cols | col1=Xero Name (RateName), col2=Xero Code (ComponentName), col3=VP Code, col4=Rate |
| Left BLANK | col3 VP Code unless VP match; col5 Compound On Code, col6 Sequence, col7 Messages |
| Match key | **(col1 RateName, col2 ComponentName)** |

```sql
SELECT xtc.RateName [XeroRateName], xtc.ComponentName [XeroComponentName], xtc.Rate [XeroRate],
       vtc.Code [VantagepointCode], vtc.Description [VantagepointName]
FROM [Xero Tax Components] xtc
LEFT JOIN [Vantagepoint Tax Codes] vtc ON xtc.RateName = vtc.Description AND xtc.ComponentName = vtc.Code
WHERE NOT EXISTS (SELECT * FROM [Mapped Tax Codes] mtc
                  WHERE mtc.XeroName = xtc.RateName AND mtc.XeroCode = xtc.ComponentName)
```

**Consumed by** `sync_tax_codes` ([03](03-sync-tax-codes.md)): placeholder rows carry RateName/ComponentName/Rate with blank VP code (col3); the sync creates the VP tax code and fills col3 (+ col5 Compound On Code, col6 Sequence).

⚠ **Notable behaviours / risks:**
- ⚠ **Likely datapill bug:** the `add_batch_of_entries` column parameters reference `…rows.first.XeroRateName` etc. (the **first** result row) while `____source` is the full `rows` array — the firms/accounts seeders correctly use `current_item`. The Airflow port **must iterate per-row** (write each row's own values). Validate against the live table before assuming Workato broadcasts `.first`.
- `isNewOrUpdated` computed but not used in seeding (it's the sync's change-detection input).
- No pagination.

---

## 4. Airflow design notes (seeding)

In the QBO `mapping_sync`, seeding is **not a separate recipe** — the `map_*_dag` engines both seed and sync in one pass (load Xero + VP + existing map → compile → upsert). The Xero Workato package splits seeding (these recipes) from sync (Initial Synch). **Two valid Airflow approaches:**

| Option | Description | Recommendation |
| --- | --- | --- |
| **A — Merge** (QBO parity) | Fold seeding into each `map_*_dag` engine: the anti-join "which Xero records aren't mapped yet" becomes part of the same compile step that the sync already runs. No separate seed task. | **Recommended** — matches the QBO reference, fewer moving parts, no `1900-01-01` sentinel needed. |
| **B — Mirror Workato** | A distinct `seed_*` task per table, gated by "table empty," writing placeholder rows, then the sync task fills them. | Only if you must preserve the exact two-phase Workato contract. |

**Either way, reproduce the decision logic:**
- Match keys for "already mapped": **Firm=ContactID, Account=XeroID, Tax=(RateName, ComponentName)** — these are the UNIQUE keys in [04-lookup-tables.md](04-lookup-tables.md).
- **Fix the three issues** in the port (don't replicate the bugs): seed firms with Vendor/Client derived from `IsSupplier`/`IsCustomer` + AccountNumber; decide whether unmapped account types should be dropped or surfaced; iterate tax rows per-item.
- **Page all Xero list calls** (contacts especially).
- The "guard if non-empty" maps to the existing `is_table_populated` skip-gate already in the QBO per-child DAG shape.
