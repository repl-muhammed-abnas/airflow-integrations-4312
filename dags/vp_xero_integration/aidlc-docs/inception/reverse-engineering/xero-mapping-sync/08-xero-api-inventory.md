# 08 — Xero API Call Inventory & RAIL Operator Gap Analysis

**Scope:** every Xero API call across the **entire** `integration_vantagepoint_xero/code/014-501 PSA` package (all 27 files with `"provider": "xero"`), attributed to the recipe using it. Produced to drive a **RAIL operator enablement** User Story (see [stories.md US-9](../../inception/stories.md)).

**RAIL today** (`replicon-airflow-library/rail/rail/operators/xero_internal/`):
- `XeroAPIOperator` — **generic** Xero caller via `XeroHook`: params `endpoint`, `request_method` (**GET/POST only** — `ALLOWED_METHODS = ['GET','POST']`), `filters` (`where=`), `request_body`, `modified_since` (`If-Modified-Since`). Handles 429 rate-limit retry. **Single request — no pagination loop.**
- `XeroRevokeTokenOperator`, `XeroHook`.

> **Bottom line:** RAIL can already call any Xero GET/POST endpoint generically. The concrete gaps are: **(1) pagination** for list reads, **(2) PUT support** for two write endpoints, and **(3) optional typed operators / a change-feed trigger** for ergonomics and the polling DAGs. **None of the gaps block the mapping_sync work** (firms/accounts/tax are all GET reads + VP-side writes).

---

## 1. Distinct Xero operations used (deduped), grouped by resource

| Xero resource | Operation (Workato) | HTTP | Used by recipe(s) | RAIL coverage |
| --- | --- | --- | --- | --- |
| **Contacts** | `get_contact_by_id` | GET `/Contacts/{id}` | upsert_firm_in_vantagepoint | ✅ generic GET |
| Contacts | `create_contact` | POST `/Contacts` | sync_employees, upsert_contact_in_xero | ✅ generic POST |
| Contacts | `update_contact` (replace; ContactStatus ACTIVE/ARCHIVED) | POST `/Contacts` | sync_employees ×2, upsert_contact_in_xero ×2 | ✅ generic POST |
| Contacts | adhoc list | GET `/Contacts` | **synch_firms** | ⚠ GET ok, **needs pagination** |
| Contacts | adhoc list incl. archived | GET `/Contacts?includeArchived=true` | **validate_firm_map** | ⚠ GET ok, **needs pagination** |
| Contacts | adhoc lookup by acct no. | GET `/Contacts?where=AccountNumber="…"` | sync_employees (`9c3b7655`, **disabled `skip:true`**) | ✅ generic GET + `filters` |
| **Accounts** | `list_accounts` | GET `/Accounts` | **map_accounts**, **validate_account_map** | ⚠ GET ok, paging (small set) |
| Accounts | `search_accounts` | GET `/Accounts` (filter Code/Status) | **sync_accounts** | ✅ generic GET + `filters` |
| Accounts | adhoc get bank account | GET `/Accounts/{id}` | resolve_bank_code (×2) | ✅ generic GET |
| **TaxRates** | `list_tax_rates` | GET `/TaxRates` | **sync_tax_codes**, **map_tax_codes**, **validate_tax_map**, xero_no_tax_code | ✅ generic GET |
| **Invoices** | `search_invoices` | GET `/Invoices` (by InvoiceNumber) | post_invoice_to_xero | ✅ generic GET |
| Invoices | `get_invoice_by_id` | GET `/Invoices/{id}` | post_invoice_to_xero, xero_bill_payment_adds | ✅ generic GET |
| Invoices | `create_invoice_with_line_item` (ACCREC) | POST `/Invoices` | post_invoice_to_xero | ✅ generic POST |
| Invoices | `create_bill_with_multiple_line_items` (ACCPAY) | POST `/Invoices` | post_ap_voucher_to_xero, post_employee_expense_to_xero | ✅ generic POST |
| **CreditNotes** | `create_credit_note` (ACCRECCREDIT) | POST `/CreditNotes` | post_invoice_to_xero | ✅ generic POST |
| CreditNotes | adhoc allocate to invoice | **PUT** `/CreditNotes/{id}/Allocations` | post_invoice_to_xero | ❌ **PUT not allowed in RAIL** |
| **ManualJournals** | `create_manual_journal` | POST `/ManualJournals` | journal_exports, revenue_generation_posts, unit_journal_exports ×2 | ✅ generic POST |
| **Payments** | `get_payment_by_id` | GET `/Payments/{id}` | xero_invoice_payment_adds, xero_bill_payment_adds | ✅ generic GET |
| **Currencies** | `list_currencies` | GET `/Currencies` | sync_currency_codes | ✅ generic GET |
| Currencies | adhoc add currency | **PUT** `/Currencies` | sync_currency_codes | ❌ **PUT not allowed in RAIL** |

### Polling **triggers** (Xero change-feed; not API operators)
| Trigger | Resource | Recipe |
| --- | --- | --- |
| `updated_contact` | Contacts (UpdatedDateUTC) | poll_xero_contact_updates_vantagepoint |
| `updated_account` | Accounts | poll_xero_chart_of_accounts |
| `updated_payment` | Payments | poll_xero_payment |

### Folders with NO Xero calls (confirmed)
`Deployment/`, `Logging/`, `Platform Monitoring/`, `Triggers - Realtime/` (all VP webhooks — Xero writes delegated to the Contacts recipes), and the Mapping root recipes (`populate_mapping_table`, `populate_mapping_state`, `premapping`, `postmapping`, `validate_mapping_tables`) + `synch_accounts`/`synch_tax_codes` (orchestrators/wrappers — no direct Xero step). The export recipes (`vantagepoint_*_exports_to_xero`, `resolve_account_code`, `resolve_tax_code`) hold only the connector-config block, no Xero call.

---

## 2. Full per-recipe attribution (every call)

| Recipe | Folder | Step `as` | Call | Method | Endpoint / resource | Purpose |
| --- | --- | --- | --- | --- | --- | --- |
| upsert_firm_in_vantagepoint | Contacts | 203eda32 | get_contact_by_id | GET | /Contacts/{id} | fetch Xero contact → upsert VP firm |
| upsert_contact_in_xero | Contacts | a94bee88 | create_contact | POST | /Contacts | create Xero contact for VP firm |
| upsert_contact_in_xero | Contacts | 701ee068 | update_contact | POST | /Contacts | update firm contact |
| upsert_contact_in_xero | Contacts | 6dba0159 | update_contact | POST | /Contacts | archive firm contact |
| sync_employees | Contacts | a0ead152 | create_contact | POST | /Contacts | create contact for VP employee |
| sync_employees | Contacts | 9c3b7655 | adhoc (disabled) | GET | /Contacts?where=AccountNumber="…" | lookup after dup error |
| sync_employees | Contacts | b115cede | update_contact | POST | /Contacts | update employee contact |
| sync_employees | Contacts | 330ccae1 | update_contact | POST | /Contacts | archive terminated employee |
| synch_firms | Mapping/Initial Synch | 5c1c538a | adhoc | GET | /Contacts | list all contacts (initial firm sync) |
| map_firms | Mapping/Lookup Tables | bf7b6a11 | adhoc | GET | /Contacts | list active contacts (seed firm map) |
| validate_firm_map | Mapping/Validation | 1a1ec79f | adhoc | GET | /Contacts?includeArchived=true | list incl. archived (validate firm map) |
| map_accounts | Mapping/Lookup Tables | 5f051edf | list_accounts | GET | /Accounts | seed account map |
| validate_account_map | Mapping/Validation | 38e690fc | list_accounts | GET | /Accounts | validate account map |
| sync_accounts | GL | cdcb327f | search_accounts | GET | /Accounts | account sync (filter Code/Status) |
| resolve_bank_code | GL | cd682b9b / c7638f57 | adhoc | GET | /Accounts/{id} | fetch bank account detail |
| map_tax_codes | Mapping/Lookup Tables | 664bb597 | list_tax_rates | GET | /TaxRates | seed tax map |
| validate_tax_map | Mapping/Validation | cd8e3446 | list_tax_rates | GET | /TaxRates | validate tax map |
| sync_tax_codes | GL | 7d88b6d4 | list_tax_rates | GET | /TaxRates | tax-code sync |
| xero_no_tax_code | GL | bbd919a8 | list_tax_rates | GET | /TaxRates | find no-tax/zero rate |
| post_invoice_to_xero | GL | e8f77f4f | search_invoices | GET | /Invoices | dedup check |
| post_invoice_to_xero | GL | 283c89e3 | get_invoice_by_id | GET | /Invoices/{id} | lookup invoice for allocation |
| post_invoice_to_xero | GL | 8794c32c | create_invoice_with_line_item | POST | /Invoices (ACCREC) | create AR invoice |
| post_invoice_to_xero | GL | 2b6d8ec7 | create_credit_note | POST | /CreditNotes | create AR credit note |
| post_invoice_to_xero | GL | 840a835f | adhoc | **PUT** | /CreditNotes/{id}/Allocations | allocate credit note |
| post_ap_voucher_to_xero | GL | 33142f83 | create_bill_with_multiple_line_items | POST | /Invoices (ACCPAY) | post AP voucher as bill |
| post_employee_expense_to_xero | GL | 9a853d37 | create_bill_with_multiple_line_items | POST | /Invoices (ACCPAY) | post expense as bill |
| vantagepoint_journal_exports_to_xero | GL | 38881682 | create_manual_journal | POST | /ManualJournals | GL journal export |
| vantagepoint_revenue_generation_posts_to_xero | GL | c5a079c1 | create_manual_journal | POST | /ManualJournals | revenue-gen journal |
| vantagepoint_unit_journal_exports_to_xero | GL | 96994df6 / bffeb6b8 | create_manual_journal | POST | /ManualJournals | unit journals (2 branches) |
| xero_invoice_payment_adds_to_vantagepoint | GL | 94358db0 | get_payment_by_id | GET | /Payments/{id} | pull invoice payment → VP |
| xero_bill_payment_adds_to_vantagepoint | GL | 0bfc168b / 20da5bf9 | get_invoice_by_id | GET | /Invoices/{id} | get bill for payment |
| xero_bill_payment_adds_to_vantagepoint | GL | cc085d40 / 962e3de3 | get_payment_by_id | GET | /Payments/{id} | get bill payment detail |
| sync_currency_codes | GL | 7d88b6d4 | list_currencies | GET | /Currencies | list currencies |
| sync_currency_codes | GL | 2a55d3ba | adhoc | **PUT** | /Currencies | add missing currency |
| poll_xero_contact_updates_vantagepoint | Triggers - Polling | 376c1a47 | **trigger** updated_contact | poll | /Contacts | contact change feed |
| poll_xero_chart_of_accounts | Triggers - Polling | baa36f2c | **trigger** updated_account | poll | /Accounts | account change feed |
| poll_xero_payment | Triggers - Polling | 3f76243e | **trigger** updated_payment | poll | /Payments | payment change feed |

---

## 3. RAIL gap analysis & recommended operator work

| # | Gap | Detail | Needed for | Priority |
| --- | --- | --- | --- | --- |
| **G1** | **Pagination** on GET list reads | `XeroAPIOperator` issues a single `requests.get` — no page loop. Xero pages Contacts (~100/page legacy, up to 1000). `synch_firms`/`map_firms`/`validate_firm_map` list all Contacts. | **mapping_sync (firm)** + all list reads | **HIGH** |
| **G2** | **PUT method support** | `ALLOWED_METHODS=['GET','POST']`. Two writes use PUT: `/CreditNotes/{id}/Allocations`, `/Currencies`. Add PUT to the operator/hook (or typed ops). | GL (invoice allocation, currency sync) | MEDIUM (not mapping_sync) |
| **G3** | **Typed convenience operators (optional)** | QBO has typed operators (Customer/Vendor/Account/TaxCode…). Equivalents: `XeroContactOperator`, `XeroAccountOperator`, `XeroTaxRateOperator`, `XeroInvoiceOperator`, `XeroCreditNoteOperator`, `XeroManualJournalOperator`, `XeroPaymentOperator`, `XeroCurrencyOperator`. Functionally optional (generic operator + endpoint works), but improves ergonomics/consistency. | ergonomics / parity | LOW |
| **G4** | **Xero change-feed trigger/sensor** | `updated_contact/account/payment` polls. Implementable today as scheduled DAGs using `XeroAPIOperator` GET + `modified_since` (`If-Modified-Since` is supported). A reusable sensor would help the polling DAGs. | Polling DAGs (out of mapping_sync) | LOW |

**Already covered (no work needed):** all GET reads (Contacts, Accounts, TaxRates, Invoices, Payments, Currencies) via generic GET + `filters` (`where=`) + `modified_since`; all POST creates/updates (Contacts incl. update via POST, Invoices/Bills, CreditNotes, ManualJournals) via generic POST + `request_body`; 429 rate-limit retry.

### Scope split (important)
- **mapping_sync (this effort: firms, accounts, tax — Q1 no employee):** needs only **GET** `/Contacts`, `/Accounts`, `/TaxRates` + VP-side writes. → **Only G1 (pagination) is a hard dependency.** Everything else is satisfied by the existing `XeroAPIOperator`.
- **Broader integration (GL transactions, polling — future efforts):** needs G2 (PUT) and benefits from G3/G4.

> See [stories.md US-9](../../inception/stories.md) for the RAIL-operator enablement story derived from this table.
