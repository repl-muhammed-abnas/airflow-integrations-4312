# 03 — Sync Tax Codes (Xero → Vantagepoint)

**Workato recipes:**
- Initial Synch wrapper: `…/Mapping/Initial Synch/014_501_psa_synch_tax_codes.recipe.json` (48 lines) — only does `call_recipe_async` → the GL recipe below.
- Real logic: `…/GL/014_501_psa_sync_tax_codes.recipe.json` ("014-501 PSA Sync Tax Codes", v3, ~5.7k lines).

**Direction:** **Xero → Vantagepoint**. Populates `014-501 PSA Map Tax Code`.
**Airflow target:** `vp_xero_integration/mapping_sync/map_tax_code_dag.py` + `utils/_tax_code_sync.py`

> **Defining characteristic:** Xero models tax as **TaxRates with nested `TaxComponents[]`**. This recipe **flattens** each rate into one row per component → **one Xero tax rate can map to several VP tax codes** (fan-out), and **compound** components are linked to their base component via VP `CompoundOnTaxCode`.

---

## 1. Trigger
GL recipe: `workato_recipe_function / execute`, no params, concurrency 1. (Wrapper calls it async.)

## 2. High-level phases
1. Init `ErrorMessage` / `CompoundError`; open try.
2. Pull Xero tax rates + `Map Tax Code`; **flatten** each rate into per-component rows (JS `FlattenTaxRates`); detect new/changed.
3. **Early exit** if nothing new/changed.
4. Build collections (Tax Code Map, Xero Tax Components, VP Tax Codes); compute max Sequence; init `Sequence`/`VantagepointCode` vars.
5. SQL join (Xero primary) computing a `CompoundOnCode` reference for compound components.
6. Foreach joined row: insert mapping if new; generate VP code + create VP tax code if unmapped; else update rate if changed; collect compound links.
7. Foreach compound link: resolve "compound on" VP code; write to mapping col5 + VP `CompoundOnTaxCode`.
8. Outer catch → `014-501 PSA Log message`.

## 3. Steps (condensed; nesting indented)
| # | Op | Logic |
| --- | --- | --- |
| 1 | declare_variable | `ErrorMessage="Failed to sync tax codes from Xero to Vantagepoint\n"`, `CompoundError` |
| 2 | **try** | wraps 3–40 |
| └3 | Xero `list_tax_rates` | → tax_rates[] with TaxComponents[], Status, TaxType, ReportTaxType |
| └4 | lookup get_entries `Map Tax Code` | col1 Xero Name, col2 Xero Code, col3 VP Code, col4 Rate, col5 Compound On Code, col6 Sequence, col7 Messages |
| └5 | js_eval **`FlattenTaxRates`** | inputs taxRates, mappedCodes → 1 row per ACTIVE rate × component; sets RateName, ComponentName, Rate, IsCompound, TaxType, ReportTaxType('none' if absent), MappedRate, VantagepointCode (match col1==Name & col2==ComponentName); **`isNewOrUpdated=true` if MappedRate≠Rate OR MappedRate=='' OR VPCode==''** |
| └6 | **if** `is_not_true(isNewOrUpdated)` | |
| &nbsp;&nbsp;└7 | **stop** (no error) | early graceful exit |
| └8 | json_parser.parse_json | typed array |
| └9–10 | build "Tax Code Map" collection | EntryID, XeroName, XeroCode, VantagepointCode, Rate, CompoundOnCode, Sequence; idx EntryID+XeroCode, secondary Sequence |
| └11 | build "Xero Tax Components" collection | RateName, ComponentName, Rate, IsCompound, TaxType, ReportTaxType, MappedRate |
| └12 | query_list | `SELECT MAX(Sequence) MaxSequence FROM "Tax Code Map"` |
| └13 | declare_variable | `Sequence = MaxSequence.presence ?? 0`, `VantagepointCode` |
| └14 | VP `tax_codes` **list** | all VP tax codes |
| └15 | build "Vantagepoint Tax Codes" collection | Code, Description, Rate, … |
| └16 | query_list (SQL join) | see below → per-row decision incl. computed `CompoundOnCode` |
| └17 | declare_list "Compound Codes" | accumulator (RateName, ComponentName, CompoundOnCode) |
| └18 | **foreach** rows | |
| &nbsp;&nbsp;└19 | **if** `blank(MappedEntryID)` | new mapping |
| &nbsp;&nbsp;&nbsp;&nbsp;└20 | lookup **add_entry** | col1=RateName, col2=ComponentName, col3=VantagepointCode, col4=Rate |
| &nbsp;&nbsp;└21 | **if** `blank(VantagepointCode)` | needs VP code + create |
| &nbsp;&nbsp;&nbsp;&nbsp;└22 | update var `Sequence = Sequence + 1` | **must precede 23** |
| &nbsp;&nbsp;&nbsp;&nbsp;└23 | update var `VantagepointCode = MappedVantagepointCode.presence \|\| 'X' + Sequence.to_s.rjust(4,'0')` | e.g. `X0007` |
| &nbsp;&nbsp;&nbsp;&nbsp;└24 | **try** | |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└25 | VP `tax_codes` **post** | Code=VantagepointCode, Description=RateName, Rate=XeroRate, **ReverseCharge**=`(ReportTaxType=='REVERSECHARGES' \|\| RateName.include?("Reverse Charge")) ? 'Y':'N'` |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└26 | lookup update_entry id=`MappedEntryID.presence ?? add_entry.id` | col3=VPCode, col4=Rate, col6=Sequence |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└27 | **if** `present(XeroCompoundOnCode)` AND `blank(MappedCompoundOnCode)` | |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└28 | insert_to_list "Compound Codes" | defer compound link |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└29–31 catch | col7 (Messages)=catch.message; `CompoundError +=` "#{VPCode} could not be added…" | |
| &nbsp;&nbsp;&nbsp;&nbsp;└32 (else) | mapped already | |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└33 | **if** `XeroRate != MappedRate` | rate changed |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└34 | VP `tax_codes` **put** | Code=MappedVPCode, Rate=XeroRate, ReverseCharge=(same formula) |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└35 | lookup update_entry | col4=Rate |
| └36 | **foreach** "Compound Codes" | resolve compound links |
| &nbsp;&nbsp;└37 | lookup **search_entries** | col1=RateName, col2=ComponentName → this component's own mapping |
| &nbsp;&nbsp;└38 | lookup **search_entries** | col1=`CompoundOnCode.split("#")[0]`, col2=`split("#")[1]` → the base code mapping |
| &nbsp;&nbsp;└39 | lookup update_entry id=search37.id | col5 = search38.entry.col3 (base VP code) |
| &nbsp;&nbsp;└40 | VP `tax_codes` **put** | Code=search37.col3, CompoundOnTaxCode=search38.col3 |
| 41–42 catch | call `014-501 PSA Log message` | ErrorMessage + CompoundError + VP catch.message |

### Step-16 SQL (Xero primary)
```sql
SELECT xtc.RateName XeroRateName, xtc.ComponentName XeroComponentName, xtc.Rate XeroRate,
       xtc.IsCompound XeroIsCompound, xtc.TaxType XeroTaxType,
       tcm.EntryID MappedEntryID, tcm.VantagepointCode MappedVantagepointCode, tcm.Rate MappedRate,
       tcm.Sequence MappedSequence, vtc.Code VantagepointCode, vtc.Description VantagepointName,
       tcm.CompoundOnCode MappedCompoundOnCode,
       (SELECT xtcsub.RateName || '#' || xtcsub.ComponentName
          FROM "Xero Tax Components" xtcsub
         WHERE xtcsub.RateName = xtc.RateName AND xtcsub.IsCompound = "f"
           AND xtcsub.ComponentName != xtc.ComponentName AND xtc.IsCompound = "t"
         LIMIT 1) CompoundOnCode,
       xtc.ReportTaxType
FROM "Xero Tax Components" xtc
LEFT JOIN "Tax Code Map" tcm ON xtc.RateName = tcm.XeroName AND xtc.ComponentName = tcm.XeroCode
LEFT JOIN "Vantagepoint Tax Codes" vtc
       ON xtc.RateName = vtc.Description AND xtc.ComponentName = vtc.Code
       OR tcm.VantagepointCode = vtc.Code
ORDER BY xtc.RateName, xtc.ComponentName, xtc.IsCompound, tcm.Sequence
```

## 4. External calls
**Xero:** `list_tax_rates` → tax_rates[]{Name, Status, TaxType, ReportTaxType, TaxComponents[]{Name, Rate, IsCompound}}. (`IsNonRecoverable` exists in raw data but is not exposed as a datapill — would need a custom action.) No pagination.
**Vantagepoint:** `tax_codes` **list / post / put**. Create writes Code, Description, Rate, ReverseCharge. Update writes Rate+ReverseCharge (step 34) or Code+CompoundOnTaxCode (step 40). Other VP tax fields left default.

## 5. Lookup table touched — `014-501 PSA Map Tax Code`
| col | label | written from |
| --- | --- | --- |
| col1 | Xero Name | Xero RateName |
| col2 | Xero Code | Xero ComponentName |
| col3 | Vantagepoint Code | generated `'X'+Sequence.rjust(4,'0')` (or reused) |
| col4 | Rate | Xero component Rate |
| col5 | Compound On Code | base component's VP code (compound pass) |
| col6 | Sequence | high-water-mark counter |
| col7 | Messages | VP create error (on failure) |

- **Join / natural key** = (col1 RateName + col2 ComponentName).
- **Upsert key for updates** = lookup `id` (`MappedEntryID.presence ?? add_entry.id`); compound pass keys by search-result id.
- **FAN-OUT:** one Xero TaxRate → one VP tax code **per TaxComponent**. Multi-component (e.g. compound) rates produce multiple mapping rows / VP codes, distinguished by ComponentName.
- **Compound linking:** the non-compound component's VP code becomes the compound component's VP `CompoundOnTaxCode`, resolved via the `RateName#ComponentName` reference (step 16 subquery) + two `search_entries` (37/38).

## 6. Matching / dedup & direction
Xero→VP. Only `Status=="ACTIVE"` rates processed (filtered in JS). `MappedEntryID` blank → new; `VantagepointCode` blank → create; rate diff → update. Compound links applied in a deferred second pass so base + compound rows both exist first.

## 7. Error handling / logging
Inner try/catch on VP create writes the error into mapping **col7 (Messages)** and appends to `CompoundError` (no throw — loop continues). Outer catch → `014-501 PSA Log message` with ErrorMessage + CompoundError + VP error + job context. Graceful `stop` when nothing changed.

## 8. Idempotency / re-run
`isNewOrUpdated` gate makes no-op runs exit early. Sequence high-water-mark prevents VP-code collisions on re-run. Existing maps hit rate-update only when `XeroRate != MappedRate`. Generated codes stable once stored (reused via `MappedVantagepointCode.presence`). Compound links overwritten idempotently.

---

## Airflow design notes (→ `map_tax_code_dag.py` / `_tax_code_sync.py`)
- **Port the `FlattenTaxRates` JS verbatim** as a Python transform (one row per ACTIVE rate × component; compute `isNewOrUpdated`). This is the load-bearing logic.
- **Reproduce the step-16 join** (incl. the compound `RateName#ComponentName` subquery) as `QueryCollectionOperator` SQL over staged collections (`xero_tax_components`, `tax_code_map`, `vp_tax_codes`).
- **Two-pass compound linking:** main foreach creates/updates codes + accumulates compound links; second foreach resolves base VP code and PUTs `CompoundOnTaxCode`. Preserve ordering.
- **Mutable run-state** (`Sequence`, `VantagepointCode`, `CompoundError`): hold in task-local state/XCom; keep "increment Sequence before deriving VantagepointCode" ordering.
- **QBO vs Xero tax model differs structurally:** QBO `_tax_code_sync.py` flattens TaxCode→TaxRate (Sales/Purchase). Xero flattens TaxRate→TaxComponent and adds **compound linking** — Xero needs its own flatten + the compound second pass (no QBO analogue). The `map_tax_code` Xero columns differ from QBO (see [04-lookup-tables.md](04-lookup-tables.md)). **Natural key = (XeroName, XeroCode) i.e. (RateName, ComponentName).**
- **VP-code generator** `'X'+Sequence.rjust(4,'0')`: reproduce; note 9999 cap.
- ⚠ **Step-16 join precedence bug** (`OR tcm.VantagepointCode = vtc.Code` not parenthesized) — fix with explicit parentheses in the port; flag in open questions.
- **`ReverseCharge`** is the only VP behavioural flag set from Xero. Confirm VP defaults for other fields are acceptable.
- **Source op:** `rail.XeroTaxRateOperator` `list` equivalent. If `IsNonRecoverable` / non-recoverable VAT is required, confirm RAIL exposes it.
