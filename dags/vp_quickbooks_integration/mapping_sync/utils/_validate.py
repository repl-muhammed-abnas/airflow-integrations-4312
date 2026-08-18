"""
Phase-5 mapping validation helpers — split out of python_callable_method.py
to keep that module under pylint's C0302 line cap.

All 4 mapping tables (firm / employee / account_code / tax_code) are
validated in one S3 download via `run_all_mapping_validations`;
`summarize_mapping_validations` aggregates the dict, applies Workato's
"trust external data (CFG_UpgradeDataSync=false)" suppression rule,
and raises RuntimeError on any hard_fail issue.

bank_code_map is not validated here — bank-code resolution is a
transaction-time concern (Workato `resolve_bank_code` lazy lookup; on
the Airflow side the invoice_payment_sync DAG resolves it from an
Airflow Variable), not a mapping-sync step.

Public surface (re-exported via `python_callable_method.py` for
backwards-compat with existing DAG imports):
    run_all_mapping_validations
    summarize_mapping_validations
"""
import rail

from vp_quickbooks_integration.common.tables import (
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
    MAP_TAX_CODE_TABLE_NAME,
)
# These three live in python_callable_method.py today (will move to
# _shared.py in a follow-up extraction). Importing them through the
# old module avoids a circular import during the staged split.
from vp_quickbooks_integration.mapping_sync.utils._shared import (
    open_mapping_collection,
    mark_step_status,
    _read_mapping_state_row,
)


def _empty_result(table_name, issue_check):
    """Build an empty-table validation result (hard_fail) for a missing
    or zero-row table."""
    return {
        'table': table_name,
        'total': 0,
        'valid': 0,
        'issues': [{
            'severity': 'hard_fail',
            'check': issue_check,
            'count': 1,
            'detail': f"{table_name} is missing or empty",
        }],
    }


def _validate_map_firm_with_cursor(cur):
    """Validate map_firm rows on an already-open cursor.

    Hard fail if empty or rows missing QBOID. A blank FirmID is NOT a
    failure: Workato parity (and the map-only forward sync in
    `_firm_sync.sync_qbo_firms_to_vp`) legitimately tracks QBO entities
    that have no VP firm as UNMAPPED rows with an empty FirmID. That
    count is surfaced as an informational warning, not a hard fail.
    Warn on invalid IsVendor.

    Cursor-taking inner; the wrapper that opens the S3 collection is
    `run_all_mapping_validations`. Splitting the open from the
    validation lets all 4 checks share one S3 download (P3 in the
    perf backlog — eliminates 3 redundant GetObject + gunzip pairs
    per validate run).
    """
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_FIRM_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_FIRM_TABLE_NAME, 'empty_table')

    unmapped_firm = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_FIRM_TABLE_NAME} "
        f"WHERE FirmID IS NULL OR FirmID = ''"
    ).fetchone()[0]
    missing_qbo_id = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_FIRM_TABLE_NAME} "
        f"WHERE QBOID IS NULL OR QBOID = ''"
    ).fetchone()[0]
    invalid_is_vendor = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_FIRM_TABLE_NAME} "
        f"WHERE IsVendor NOT IN ('Y', 'N')"
    ).fetchone()[0]

    issues = []
    if unmapped_firm:
        # Informational only — a QBO entity with no VP firm (Workato
        # parity). Not a failure; surfaced so operators can see how many
        # QBO records are awaiting a VP firm mapping.
        issues.append({
            'severity': 'warn',
            'check': 'unmapped_firm',
            'count': unmapped_firm,
            'detail': 'QBO entities with no VP firm (blank FirmID); '
                      'expected for QBO-native / not-yet-mapped records',
        })
    if missing_qbo_id:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_qbo_id',
            'count': missing_qbo_id,
        })
    if invalid_is_vendor:
        issues.append({
            'severity': 'warn',
            'check': 'invalid_is_vendor_flag',
            'count': invalid_is_vendor,
        })

    # Blank-FirmID rows are valid (unmapped tracking rows); only a
    # missing QBOID makes a row invalid.
    valid = total - missing_qbo_id
    return {
        'table': MAP_FIRM_TABLE_NAME,
        'total': total,
        'valid': valid,
        'issues': issues,
    }


def _validate_map_employee_with_cursor(cur):
    """Validate map_employee rows on an already-open cursor.

    Hard fail if empty or rows missing Employee, QBOID, or QBOVendorID.
    The QBOVendorID coverage check is what Phase 5 calls 'expense
    readiness'.
    """
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_EMPLOYEE_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_EMPLOYEE_TABLE_NAME, 'empty_table')

    missing_employee = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_EMPLOYEE_TABLE_NAME} "
        f"WHERE Employee IS NULL OR Employee = ''"
    ).fetchone()[0]
    missing_qbo_id = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_EMPLOYEE_TABLE_NAME} "
        f"WHERE QBOID IS NULL OR QBOID = ''"
    ).fetchone()[0]
    missing_qbo_vendor_id = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_EMPLOYEE_TABLE_NAME} "
        f"WHERE QBOVendorID IS NULL OR QBOVendorID = ''"
    ).fetchone()[0]

    issues = []
    if missing_employee:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_employee_id',
            'count': missing_employee,
        })
    if missing_qbo_id:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_qbo_id',
            'count': missing_qbo_id,
        })
    if missing_qbo_vendor_id:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_qbo_vendor_id',
            'count': missing_qbo_vendor_id,
            'detail': 'employees without a paired QBO Vendor can\'t process expenses',
        })

    valid = total - max(missing_employee, missing_qbo_id,
                        missing_qbo_vendor_id)
    return {
        'table': MAP_EMPLOYEE_TABLE_NAME,
        'total': total,
        'valid': valid,
        'issues': issues,
    }


def _validate_map_account_code_with_cursor(cur):
    """Validate map_account_code rows on an already-open cursor.

    Hard fail if empty or rows missing QBOID. A blank VantagepointCode is NOT
    a failure: Similar to map_firm (Workato parity), QBO accounts that have no
    VP mapping are legitimately tracked as UNMAPPED rows with an empty
    VantagepointCode. That count is surfaced as an informational warning, not
    a hard fail. Warn if no QBO 'Accounts Payable' is mapped (the AP Liability
    critical-account check).
    """
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_ACCOUNT_CODE_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_ACCOUNT_CODE_TABLE_NAME, 'empty_table')

    unmapped_account = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_ACCOUNT_CODE_TABLE_NAME} "
        f"WHERE VantagepointCode IS NULL OR VantagepointCode = ''"
    ).fetchone()[0]
    missing_qbo_id = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_ACCOUNT_CODE_TABLE_NAME} "
        f"WHERE QBOID IS NULL OR QBOID = ''"
    ).fetchone()[0]
    ap_liability_rows = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_ACCOUNT_CODE_TABLE_NAME} "
        f"WHERE QBOType = 'Accounts Payable'"
    ).fetchone()[0]

    issues = []
    if unmapped_account:
        # Informational only — a QBO account with no VP mapping (similar to
        # firm validation logic). Not a failure; surfaced so operators can
        # see how many QBO accounts are awaiting a VP mapping.
        issues.append({
            'severity': 'warn',
            'check': 'unmapped_account',
            'count': unmapped_account,
            'detail': 'QBO accounts with no VP mapping (blank VantagepointCode); '
                      'expected for QBO-native / not-yet-mapped records',
        })
    if missing_qbo_id:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_qbo_id',
            'count': missing_qbo_id,
        })
    if ap_liability_rows == 0:
        issues.append({
            'severity': 'warn',
            'check': 'no_ap_liability_mapped',
            'count': 1,
            'detail': "no row in map_account_code has QBOType='Accounts Payable' — AP voucher posting may not have a target",
        })

    # Blank-VantagepointCode rows are valid (unmapped tracking rows); only a
    # missing QBOID makes a row invalid.
    valid = total - missing_qbo_id
    return {
        'table': MAP_ACCOUNT_CODE_TABLE_NAME,
        'total': total,
        'valid': valid,
        'issues': issues,
    }


def _validate_map_tax_code_with_cursor(cur):
    """Validate map_tax_code rows on an already-open cursor.

    Hard fail if empty or rows missing QBOCodeID, VantagepointCode, or
    QBORateID. Warn if Rate doesn't parse as a number.
    """
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_TAX_CODE_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_TAX_CODE_TABLE_NAME, 'empty_table')

    missing_qbo_code_id = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_TAX_CODE_TABLE_NAME} "
        f"WHERE QBOCodeID IS NULL OR QBOCodeID = ''"
    ).fetchone()[0]
    missing_vp_code = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_TAX_CODE_TABLE_NAME} "
        f"WHERE VantagepointCode IS NULL OR VantagepointCode = ''"
    ).fetchone()[0]
    missing_qbo_rate_id = cur.execute(
        f"SELECT COUNT(*) FROM {MAP_TAX_CODE_TABLE_NAME} "
        f"WHERE QBORateID IS NULL OR QBORateID = ''"
    ).fetchone()[0]

    # Rate sanity: scan all rates and count how many fail float parse.
    unparseable_rates = 0
    for (rate_raw,) in cur.execute(
        f'SELECT Rate FROM {MAP_TAX_CODE_TABLE_NAME}'
    ).fetchall():
        if rate_raw in (None, ''):
            continue
        try:
            float(rate_raw)
        except (TypeError, ValueError):
            unparseable_rates += 1

    issues = []
    if missing_qbo_code_id:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_qbo_code_id',
            'count': missing_qbo_code_id,
        })
    if missing_vp_code:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_vantagepoint_code',
            'count': missing_vp_code,
        })
    if missing_qbo_rate_id:
        issues.append({
            'severity': 'hard_fail',
            'check': 'missing_qbo_rate_id',
            'count': missing_qbo_rate_id,
        })
    if unparseable_rates:
        issues.append({
            'severity': 'warn',
            'check': 'unparseable_rate_value',
            'count': unparseable_rates,
            'detail': 'Rate does not parse as a float',
        })

    valid = total - max(
        missing_qbo_code_id, missing_vp_code, missing_qbo_rate_id,
    )
    return {
        'table': MAP_TAX_CODE_TABLE_NAME,
        'total': total,
        'valid': valid,
        'issues': issues,
    }


def run_all_mapping_validations():
    """Run all 4 mapping-table validations on a single open S3 collection.

    Replaces the prior 4-parallel-PythonOperator pattern (one validator
    per task, each independently downloading + decompressing the
    customer's `collections.db.gz`). The collapsed shape does one
    download + 4 cursor-scoped validation passes, then returns a single
    dict consumed by `summarize_mapping_validations` via XCom.

    P3 in the perf backlog. Also a prerequisite for the
    `BatchTaskRunOperator` wrap on `validate_mappings_dag` — the
    operator's parallel-task precheck would reject the original 4-way
    fan-in into `summarize`.

    Returns: `{'map_firm': {...}, 'map_employee': {...},
               'map_account_code': {...}, 'map_tax_code': {...}}`.
    Each inner dict has the shape that the per-table validators
    returned before P3 (`table`, `total`, `valid`, `issues`).
    """
    with open_mapping_collection(read_only=True) as conn:
        cur = conn.cursor()
        return {
            'map_firm':         _validate_map_firm_with_cursor(cur),
            'map_employee':     _validate_map_employee_with_cursor(cur),
            'map_account_code': _validate_map_account_code_with_cursor(cur),
            'map_tax_code':     _validate_map_tax_code_with_cursor(cur),
        }


def summarize_mapping_validations():
    """Aggregate the 4 validation results, log a structured report,
    and raise RuntimeError if any 'hard_fail' issue was reported.

    Reads a single dict from `rail.result('run_all_mapping_validations')`
    keyed by table name (post-P3 shape). Hard-fail issues block the
    dispatcher's mark_mapping_init_complete via the existing gather →
    has_sync_errors → fail_mapping_sync chain. Warn-severity issues are
    logged but don't fail the run.
    """
    context = rail.get_current_context()
    log = context['task_instance'].log

    table_names = ('map_firm', 'map_employee',
                   'map_account_code', 'map_tax_code')
    bundle = rail.result('run_all_mapping_validations')

    summary = {
        'totals': {},
        'hard_fails': [],
        'warnings': [],
    }
    if not isinstance(bundle, dict):
        for table_name in table_names:
            summary['hard_fails'].append({
                'table': table_name,
                'check': 'validator_returned_no_result',
                'count': 1,
                'detail': (
                    f"rail.result('run_all_mapping_validations') "
                    f"returned {bundle!r}"
                ),
            })
        bundle = {}

    for table_name in table_names:
        result = bundle.get(table_name)
        if not isinstance(result, dict):
            summary['hard_fails'].append({
                'table': table_name,
                'check': 'validator_returned_no_result',
                'count': 1,
                'detail': f'bundle[{table_name!r}] = {result!r}',
            })
            continue
        summary['totals'][table_name] = {
            'total': result.get('total', 0),
            'valid': result.get('valid', 0),
        }
        for issue in result.get('issues', []) or []:
            tagged = {**issue, 'table': table_name}
            if issue.get('severity') == 'hard_fail':
                summary['hard_fails'].append(tagged)
            else:
                summary['warnings'].append(tagged)

    # Workato parity: when premapping marked a step Status='Complete'
    # (CFG_UpgradeDataSync=false → external data assumed, sync skipped),
    # the validators see empty tables and report "empty mapping table"
    # hard_fail. Workato's validate_mapping_tables doesn't fire in that
    # case — the integration trusts the external setup. Distinguish
    # premapping-Complete from sync-Complete by inspecting Messages
    # (premapping writes 'premapping' into the column; mark_step_status
    # from a successful sync writes '' by default).
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_STEP_FIRM, MAPPING_STEP_EMPLOYEE,
        MAPPING_STEP_ACCOUNT, MAPPING_STEP_TAX_CODE,
    )
    table_to_step = {
        'map_firm': MAPPING_STEP_FIRM,
        'map_employee': MAPPING_STEP_EMPLOYEE,
        'map_account_code': MAPPING_STEP_ACCOUNT,
        'map_tax_code': MAPPING_STEP_TAX_CODE,
    }
    premapping_skipped_tables = set()
    for table_name, step in table_to_step.items():
        try:
            status, messages = _read_mapping_state_row(step)
        except Exception:  # pylint: disable=broad-exception-caught
            status, messages = '', ''
        if status == 'Complete' and 'premapping' in messages.lower():
            premapping_skipped_tables.add(table_name)
    if premapping_skipped_tables:
        retained = []
        suppressed = []
        for issue in summary['hard_fails']:
            if issue.get('table') in premapping_skipped_tables:
                suppressed.append(issue)
            else:
                retained.append(issue)
        if suppressed:
            log.info(
                "Suppressed %d hard_fail issue(s) for tables %s — "
                "premapping marked Status='Complete' (external data "
                "assumed; CFG_UpgradeDataSync=false path)",
                len(suppressed), sorted(premapping_skipped_tables),
            )
        summary['hard_fails'] = retained
        summary['premapping_skipped'] = sorted(premapping_skipped_tables)

    log.info("validate_mappings summary: %s", summary)

    for warning in summary['warnings']:
        log.warning("Mapping validation warning: %s", warning)

    # Workato parity: per-step Status='Error' on validation failure
    # (recipe `014_503_psa_validate_mapping_tables.recipe.json` lines
    # 376/814/1253/1691). One Error mark per distinct failing table.
    if summary['hard_fails']:
        marked_steps = set()
        for issue in summary['hard_fails']:
            step = table_to_step.get(issue.get('table'))
            if step and step not in marked_steps:
                try:
                    mark_step_status(
                        step, 'Error',
                        message=f"{issue.get('check')}: {issue.get('detail')}"[
                            :500],
                    )
                    marked_steps.add(step)
                except Exception as mark_exc:  # pylint: disable=broad-exception-caught
                    log.error(
                        "Failed to mark step %r Status='Error': %s",
                        step, mark_exc,
                    )

        first = summary['hard_fails'][0]
        raise RuntimeError(
            f"validate_mappings had {len(summary['hard_fails'])} hard_fail "
            f"issue(s); first: {first}"
        )
    return summary
