# CFG_* migration — middleware payload → airflow

The middleware integration response carries per-tenant configuration
under a nested `config` object on each integration row, e.g.:

```json
{
  "customerId": "Cust-0012",
  "integrationType": "mapping_sync",
  "config": {
    "CFG_Region": "UK",
    "CFG_NoTaxCode": "No Vat",
    "CFG_UpgradeDataSync": false,
    "CFG_DefaultVendorType": "C",
    "CFG_DefaultPaymentPeriod": 30,
    "CFG_DefaultEmployeeLaborType": "E",
    "CFG_InvoiceNotificationEmail": "chetanthorat@deltek.com",
    "CFG_DisableTimesheetIntegration": false
  }
}
```

`main_dag.py` ships this dict in the dispatcher's `dag_run.conf`, and
`build_child_dag_conf()` in `utils/python_callable_method.py` forwards
the whole `config` block (plus a CFG-resolved `region`) to every child
DAG.

## Resolution order

For per-tenant defaults, the codebase uses
`_resolve_cfg_then_variable(cfg_key, variable_name)` which resolves in
this order:

1. **`dag_run.conf['config'][cfg_key]`** — middleware integration
   payload (the new, intended source).
2. **`Airflow Variable[variable_name]`** — per-instance legacy
   Variable, kept for backwards compatibility and as the fallback for
   CFG keys the middleware doesn't ship yet.
3. **`None`** — handled by the caller (most body builders drop the
   field via `_filter_none`; some have a VP-side fallback).

## CFG → code mapping

### Wired in mapping_sync today

| CFG key | Workato recipe ref | Resolver | Used by |
|---|---|---|---|
| `CFG_Region` | regional address/tax branches | `build_child_dag_conf()` → child's `region` field | Threaded to each child DAG's conf; future regional branches in body builders |
| `CFG_DefaultVendorType` | `014_503_psa_quickbooks_customer_vendor_to_vantagepoint` | `lookup_default_vendor_type(instance)` | Firm `Category` (POST body line 326 + Category backfill PUT line 1098) |
| `CFG_DefaultEmployeeLaborType` | `014_503_psa_vantagepoint_upsert_employee` line 1018 | `lookup_default_employee_labor_type(instance)` | Employee `Type` field |
| `CFG_DefaultOrganization` | `014_503_psa_vantagepoint_upsert_employee` line 1010 | `lookup_default_organization(instance)` | Employee `Org` field — falls back to `_fetch_first_vp_organization_org` (VP-side `/api/organization` first-row) when None |
| `CFG_UpgradeDataSync` | `014_503_psa_premapping` initial-vs-upgrade branch | `apply_premapping_state()` in dispatcher | Drives `mapping_table_state.Status` per the 5 mapping steps (the 4 Workato-parity steps + the Airflow-only bank_code step). `false` (Workato default) → Status='Complete' (child DAGs skip their sync); `true` → Status='' (force re-run). Matches recipe lines 436-454. **Airflow-side content-aware override** (not in Workato): when `CFG=false` but all 5 mapping tables are empty (fresh customer, or operator deleted the S3 collection to force a re-run), the override applies `CFG=true` semantics — Status='' so syncs run. Strict Workato "trust existing data" skip is preserved only when the tables actually have content. This makes "delete S3 collection + reset `vp_qbo_mapping_init_*` Variable" the natural way to force a re-sync without flipping the middleware-side CFG flag. The `apply_premapping_state` task surfaces `content_override`, `is_upgrade_effective`, and `cfg_upgrade_data_sync` on each per-step result dict so the middleware payload from `post_dag_run_details` can read the override decision without parsing `messages`. |

### Deferred (middleware ships them today, mapping_sync doesn't use them yet)

| CFG key | Workato recipe ref | Consumer once wired | Plan |
|---|---|---|---|
| `CFG_DefaultEmployeeCompany` | `014_503_psa_vantagepoint_upsert_employee` line 1011 | Employee `EmployeeCompany` field | Re-author `lookup_default_employee_company(instance)` (resolves `CFG_DefaultEmployeeCompany` → Variable `vp_qbo_mapping_sync_default_employee_company_{instance}`) and replace the hardcoded `'EmployeeCompany': ''` in `build_vp_employee_create_body_from_qbo` (~line 1795) with `lookup_default_employee_company(instance) or ''`. Helper was removed in D2 cleanup because the create body wasn't actually consuming it. |
| `CFG_DefaultHomeCompany` | `014_503_psa_vantagepoint_upsert_employee` line 1012 | Employee `HomeCompany` field | Same shape as above for the `HomeCompany` field (line 1794 in the create body). Re-author `lookup_default_home_company` when wiring. |
| `CFG_NoTaxCode` | `014_503_psa_resolve_tax_code` | Tax-code fallback in AP/AR voucher flows | Wire when post-mapping AP/AR voucher DAGs land |
| `CFG_DefaultPaymentPeriod` | `014_503_psa_dvp_insert_update_veaccounting` | Default pay-terms days on `VendorAccountingInfo` | Wire into `_build_veaccounting_body` (firm sync) when the trial requires it |
| `CFG_DisableTimesheetIntegration` | `014_503_psa_poll_vantagepoint_timesheets` | Skip-gate for `timesheets_sync` DAG | Out of scope for mapping_sync; wire in `timesheets_sync/dispatcher_dag.py` |
| `CFG_InvoiceNotificationEmail` | `014_503_psa_send_error_notification_email` | Error notification recipient | Wire when error-notification email task is added |

## Variable.get sites NOT migrated (intentional)

| Variable | Why it stays |
|---|---|
| `vp_qbo_{customerId}_mapping_init` | Per-customer one-shot init gate — sync state, not config |
| `middleware_api_base_url` | Cross-DAG infra endpoint, not per-tenant |
| `vp_qbo_mapping_sync_schedule_interval_{instance}` | Airflow-level DAG schedule |
| `vantagepoint_client_id` / `vantagepoint_client_secret` | OAuth credentials (secrets — middleware doesn't ship them) |
| `vp_qbo_vendor_sync_pay_terms_map` | Tenant pay-terms mapping table (different shape than `CFG_DefaultPaymentPeriod`) |

## Rolling out a new CFG key

1. Confirm the middleware integration payload ships the key.
2. If a Workato Variable equivalent doesn't already exist in
   `python_callable_method.py`, add a `lookup_default_<thing>(instance)`
   that calls `_resolve_cfg_then_variable('CFG_<Name>', '<variable_name>')`.
3. Reference it from the relevant body builder / branch.
4. Document the new entry in this file (move it from "Deferred" to
   "Wired").
5. Update the relevant `MAP_<TABLE>_SYNC_FIX_LOG.md` if the resolution
   path materially changes a sync outcome.
