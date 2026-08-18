"""
Phase-5 mapping validation (Xero → VP).

Read-only referential-integrity checks on the three mapping tables (firm /
account / tax) per reverse-engineering doc 07-validation.md. Each table is
anti-joined against freshly-fetched Xero + VP data (fetched by the
validate_mappings DAG and read via `rail.result`) inside ONE read-only S3 open.

Behaviour decisions (Q5 / Q-V1):
  - Validation is READ-ONLY/reporting. Self-heal (account col6/col7 refresh) and
    archived-contact cleanup live in the sync engines, not here.
  - Signalling is state-driven (NOT the Workato return-string): on any hard_fail
    the failing table's `mapping_table_state.Status` is set to 'Error' and a
    RuntimeError is raised so the dispatcher's gather → has_sync_errors →
    fail_mapping_sync chain leaves the per-customer init Variable at 'false'.
  - map_firm empty-table is a WARNING (not hard_fail): Workato's firm validator
    always returns blank (Q-V1 bug), meaning firm validation never blocks. An
    empty firm map after a successful sync (no Xero contacts matched VP firms)
    is a valid state. Dangling-reference issues for map_firm still surface as
    hard_fail.
  - Premapping-skipped tables (apply_premapping_state marked Status='Complete'
    with 'premapping' in Messages because the table already had data) have their
    empty-table hard_fails suppressed.

Public surface (re-exported via `python_callable_method.py`):
    run_all_mapping_validations
    summarize_mapping_validations
"""
import rail

from vp_xero_integration_v2.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
    MAP_TAX_CODE_TABLE_NAME,
)
from vp_xero_integration_v2.mapping_sync.utils._shared import (
    open_mapping_collection,
    mark_step_status,
    _read_mapping_state_row,
    _extract_xero_records,
)
from vp_xero_integration_v2.common.python_callable_method import unwrap_vp_response
from vp_xero_integration_v2.mapping_sync.utils._tax_code_sync import (
    flatten_xero_tax_rates,
)

# Cap how many sample identifiers we attach to an issue's detail string.
_SAMPLE_LIMIT = 10


def _empty_result(table_name, severity='hard_fail'):
    """Empty/missing table issue. severity defaults to hard_fail; pass 'warning'
    for tables where an empty result after a successful sync is acceptable."""
    return {
        'table': table_name,
        'total': 0,
        'valid': 0,
        'issues': [{
            'severity': severity,
            'check': 'empty_table',
            'count': 1,
            'detail': f"{table_name} is missing or empty",
        }],
    }


def _dangling_issue(check, dangling, detail_prefix):
    """Build a hard_fail issue for a list of dangling identifiers."""
    sample = ', '.join(str(d) for d in dangling[:_SAMPLE_LIMIT])
    more = '' if len(dangling) <= _SAMPLE_LIMIT else f' (+{len(dangling) - _SAMPLE_LIMIT} more)'
    return {
        'severity': 'hard_fail',
        'check': check,
        'count': len(dangling),
        'detail': f"{detail_prefix}: {sample}{more}",
    }


def _validate_map_firm_with_cursor(cur, live_contact_ids, live_vp_firm_ids):
    """map_firm referential checks (doc 07 §1):
      - ContactID resolves to a live Xero contact (excl. blank + ARCHIVED rows).
      - FirmID resolves to a live VP firm (excl. blank + ARCHIVED rows).
    Archived-row cleanup is the firm sync's job, so ARCHIVED rows are skipped.
    Empty table → warning (not hard_fail): mirrors Workato Q-V1 behaviour where
    the firm validator always returns blank and never blocks the pipeline."""
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_FIRM_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_FIRM_TABLE_NAME, severity='warning')

    dangling_contacts, dangling_firms = [], []
    for firm_id, contact_id, status, xero_name, vp_name in cur.execute(
        f'SELECT FirmID, ContactID, Status, XeroName, VantagepointName '
        f'FROM {MAP_FIRM_TABLE_NAME}'
    ).fetchall():
        if str(status or '').strip().upper() == 'ARCHIVED':
            continue
        if contact_id and str(contact_id) not in live_contact_ids:
            dangling_contacts.append(f'{xero_name} ({contact_id})')
        if firm_id and str(firm_id) not in live_vp_firm_ids:
            dangling_firms.append(f'{vp_name} ({firm_id})')

    issues = []
    if dangling_contacts:
        issues.append(_dangling_issue(
            'dangling_xero_contact', dangling_contacts,
            'map_firm ContactIDs not found in Xero'))
    if dangling_firms:
        issues.append(_dangling_issue(
            'dangling_vp_firm', dangling_firms,
            'map_firm FirmIDs not found in Vantagepoint'))
    valid = total - max(len(dangling_contacts), len(dangling_firms))
    return {'table': MAP_FIRM_TABLE_NAME, 'total': total,
            'valid': valid, 'issues': issues}


def _validate_map_chart_of_accounts_with_cursor(cur, live_vp_account_codes,
                                                live_xero_account_ids):
    """map_chart_of_accounts referential checks (doc 07 §2):
      - VantagepointCode resolves to a live VP account (excl. blank).
      - XeroID resolves to a live Xero account (excl. blank)."""
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_CHART_OF_ACCOUNTS_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_CHART_OF_ACCOUNTS_TABLE_NAME)

    dangling_vp, dangling_xero = [], []
    for xero_code, xero_name, vp_code, xero_id in cur.execute(
        f'SELECT XeroCode, XeroName, VantagepointCode, XeroID '
        f'FROM {MAP_CHART_OF_ACCOUNTS_TABLE_NAME}'
    ).fetchall():
        if vp_code and str(vp_code) not in live_vp_account_codes:
            dangling_vp.append(f'{xero_name} ({vp_code})')
        if xero_id and str(xero_id) not in live_xero_account_ids:
            dangling_xero.append(f'{xero_name} ({xero_code})')

    issues = []
    if dangling_vp:
        issues.append(_dangling_issue(
            'dangling_vp_account', dangling_vp,
            'map_chart_of_accounts VantagepointCodes not found in Vantagepoint'))
    if dangling_xero:
        issues.append(_dangling_issue(
            'dangling_xero_account', dangling_xero,
            'map_chart_of_accounts XeroIDs not found in Xero'))
    valid = total - max(len(dangling_vp), len(dangling_xero))
    return {'table': MAP_CHART_OF_ACCOUNTS_TABLE_NAME, 'total': total,
            'valid': valid, 'issues': issues}


def _validate_map_tax_code_with_cursor(cur, live_vp_tax_codes,
                                       live_xero_components):
    """map_tax_code referential checks (doc 07 §3):
      - VantagepointCode resolves to a live VP tax code (excl. blank).
      - (XeroName, XeroCode) resolves to a live ACTIVE Xero rate/component."""
    total = cur.execute(
        f'SELECT COUNT(*) FROM {MAP_TAX_CODE_TABLE_NAME}'
    ).fetchone()[0]
    if total == 0:
        return _empty_result(MAP_TAX_CODE_TABLE_NAME)

    dangling_vp, dangling_xero = [], []
    for xero_name, xero_code, vp_code in cur.execute(
        f'SELECT XeroName, XeroCode, VantagepointCode '
        f'FROM {MAP_TAX_CODE_TABLE_NAME}'
    ).fetchall():
        if vp_code and str(vp_code) not in live_vp_tax_codes:
            dangling_vp.append(f'{xero_name}/{xero_code} ({vp_code})')
        if (xero_name, xero_code) not in live_xero_components:
            dangling_xero.append(f'{xero_name}/{xero_code}')

    issues = []
    if dangling_vp:
        issues.append(_dangling_issue(
            'dangling_vp_tax_code', dangling_vp,
            'map_tax_code VantagepointCodes not found in Vantagepoint'))
    if dangling_xero:
        issues.append(_dangling_issue(
            'dangling_xero_rate_component', dangling_xero,
            'map_tax_code (RateName, ComponentName) not found among ACTIVE Xero rates'))
    valid = total - max(len(dangling_vp), len(dangling_xero))
    return {'table': MAP_TAX_CODE_TABLE_NAME, 'total': total,
            'valid': valid, 'issues': issues}


def _live_id_set(task_id, key, extractor=_extract_xero_records):
    """Build a set of str(record[key]) from a fetched source-task result."""
    return {
        str(r.get(key))
        for r in extractor(rail.result(task_id))
        if isinstance(r, dict) and r.get(key) not in (None, '')
    }


def run_all_mapping_validations():
    """Run all three mapping-table referential validations in one read-only S3
    open. Live Xero/VP data is read from the validate DAG's fetch tasks via
    `rail.result`. Returns a bundle keyed by table name."""
    live_contact_ids = _live_id_set('validate_fetch_xero_contacts', 'ContactID')
    live_vp_firm_ids = _live_id_set('validate_fetch_vp_firms', 'ClientID', unwrap_vp_response)
    live_vp_account_codes = _live_id_set('validate_fetch_vp_accounts', 'Account', unwrap_vp_response)
    live_xero_account_ids = _live_id_set('validate_fetch_xero_accounts', 'AccountID')
    live_vp_tax_codes = _live_id_set('validate_fetch_vp_tax_codes', 'Code', unwrap_vp_response)
    live_xero_components = {
        (row['RateName'], row['ComponentName'])
        for row in flatten_xero_tax_rates(
            _extract_xero_records(rail.result('validate_fetch_xero_tax_rates')))
    }

    with open_mapping_collection(read_only=True) as conn:
        cur = conn.cursor()
        return {
            'map_firm': _validate_map_firm_with_cursor(
                cur, live_contact_ids, live_vp_firm_ids),
            'map_chart_of_accounts': _validate_map_chart_of_accounts_with_cursor(
                cur, live_vp_account_codes, live_xero_account_ids),
            'map_tax_code': _validate_map_tax_code_with_cursor(
                cur, live_vp_tax_codes, live_xero_components),
        }


def summarize_mapping_validations():
    """Aggregate the validation bundle, suppress premapping-skipped empties, mark
    failing steps Status='Error', and raise RuntimeError on any hard_fail."""
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
        MAPPING_STEP_FIRM, MAPPING_STEP_ACCOUNT, MAPPING_STEP_TAX_CODE,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    table_to_step = {
        'map_firm': MAPPING_STEP_FIRM,
        'map_chart_of_accounts': MAPPING_STEP_ACCOUNT,
        'map_tax_code': MAPPING_STEP_TAX_CODE,
    }
    table_names = tuple(table_to_step)
    bundle = rail.result('run_all_mapping_validations')

    summary = {'totals': {}, 'hard_fails': [], 'warnings': []}
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

    # Premapping suppression: when apply_premapping_state marked a step
    # Status='Complete' because the table already had data, an empty-table
    # hard_fail from the validator is a false positive — suppress it.
    premapping_skipped = set()
    for table_name, step in table_to_step.items():
        try:
            status, messages = _read_mapping_state_row(step)
        except Exception:  # pylint: disable=broad-exception-caught
            status, messages = '', ''
        if status == 'Complete' and 'premapping' in (messages or '').lower():
            premapping_skipped.add(table_name)
    if premapping_skipped:
        retained, suppressed = [], []
        for issue in summary['hard_fails']:
            (suppressed if issue.get('table') in premapping_skipped
             and issue.get('check') == 'empty_table' else retained).append(issue)
        if suppressed:
            log.info(
                "Suppressed %d empty-table hard_fail(s) for premapping-skipped "
                "tables %s (premapping marked Complete — table had data)",
                len(suppressed), sorted(premapping_skipped))
        summary['hard_fails'] = retained
        summary['premapping_skipped'] = sorted(premapping_skipped)

    log.info("validate_mappings summary: %s", summary)
    for warning in summary['warnings']:
        log.warning("Mapping validation warning: %s", warning)

    if summary['hard_fails']:
        marked_steps = set()
        for issue in summary['hard_fails']:
            step = table_to_step.get(issue.get('table'))
            if step and step not in marked_steps:
                try:
                    mark_step_status(
                        step, 'Error',
                        message=f"{issue.get('check')}: {issue.get('detail')}"[:500])
                    marked_steps.add(step)
                except Exception as mark_exc:  # pylint: disable=broad-exception-caught
                    log.error("Failed to mark step %r Status='Error': %s",
                              step, mark_exc)
        first = summary['hard_fails'][0]
        raise RuntimeError(
            f"validate_mappings had {len(summary['hard_fails'])} hard_fail "
            f"issue(s); first: {first}")
    return summary
