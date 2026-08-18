"""
Common utility methods for VP -> QBO Timesheets Sync.

Translates the Workato recipes (poll, orchestrator, post) into Python
callables for the 4-DAG Airflow template
(main -> dispatcher -> router -> time_activity_create). Error reporting
goes to middleware via PostDagRunDetailsToMiddlewareApiOperator +
FailOperator on the dispatcher's failure branch; no email/CSV path
here (matches vendor_sync and customer_sync).
"""
# pylint: disable=invalid-name,broad-exception-caught
import logging
import re
from datetime import datetime, timezone
from airflow.models import Variable
import rail
from vp_quickbooks_integration.timesheets_sync.config import (
    billing_transfer_marker,
    initial_sync_time,
    s3_integration_name,
    s3_mapping_integration_type,
)
from vp_quickbooks_integration.common.python_callable_method import (
    watermark_key_template,
)
from vp_quickbooks_integration.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME as map_employee_table_name,
    MAP_FIRM_TABLE_NAME as map_firm_table_name,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watermark helpers (replaces Workato `polling_PSALedger_updated` ModifiedDate
# tracking). One Variable per customerId.
# ---------------------------------------------------------------------------
WATERMARK_VARIABLE_KEY_TEMPLATE = watermark_key_template('timesheets_sync')


_CUSTOMER_ID_SAFE_RE = re.compile(r'[^A-Za-z0-9_-]')


def _sanitize_customer_id(customer_id):
    """Strip Airflow-Variable-unsafe chars; fall back to 'default' when empty."""
    if not customer_id:
        return 'default'
    cleaned = _CUSTOMER_ID_SAFE_RE.sub('_', str(customer_id))
    return cleaned or 'default'


def _qbo_capability_enabled(capability):
    """Per-tenant QBO realm capability flag.

    QBO rejects feature-gated fields unless the realm has the feature
    enabled (e.g. BillableStatus requires Time-Tracking). We read a
    Variable per (instance, customer_id) maintained by ops. Default
    False keeps day-one onboarding from crashing.

    Lookup order: qbo_{capability}_enabled_{instance}_{customer_id},
    then qbo_{capability}_enabled_{instance}, then False.
    """
    try:
        ctx = rail.get_current_context()
        dag_id = (ctx.get('dag').dag_id if ctx.get('dag') else '') or ''
        instance = dag_id.rsplit('_', 1)[-1] if '_' in dag_id else ''
        conf = (ctx.get('dag_run').conf if ctx.get('dag_run') else None) or {}
        customer_id = _sanitize_customer_id(conf.get('customerId'))
    except Exception:  # pylint: disable=broad-exception-caught
        return False
    for key in (
        f'qbo_{capability}_enabled_{customer_id}_{instance}' if instance else None,
        f'qbo_{capability}_enabled_{instance}' if instance else None,
    ):
        if not key:
            continue
        raw = Variable.get(key, default_var=None)
        if raw is None:
            continue
        return str(raw).lower() in ('1', 'true', 'yes', 'on')
    return False


def _watermark_variable_key(instance, customer_id):
    return WATERMARK_VARIABLE_KEY_TEMPLATE.format(
        instance=instance,
        customer_id=_sanitize_customer_id(customer_id),
    )


def _utc_now_iso():
    """ISO-8601 millisecond UTC timestamp with 'Z' suffix."""
    return (
        datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        + 'Z'
    )


def prepare_sync_timestamps_method(instance):
    """Capture last sync time + current time for the OData filter."""
    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = _watermark_variable_key(instance, customer_id)
    current_time = _utc_now_iso()
    try:
        last_sync_time = Variable.get(key)
        print(f"Retrieved last sync time from Variable '{key}': "
              f"{last_sync_time}")
    except KeyError:
        last_sync_time = initial_sync_time
        print(f"Variable '{key}' not found, using initial sync time: "
              f"{last_sync_time}")
    return {
        'last_sync_time': last_sync_time,
        'current_sync_time': current_time,
    }


def update_last_sync_time_method(instance):
    """
    Persist `current_sync_time` into the Variable after run completes.

    trigger_rule='all_done' on the dispatcher task means this callable
    runs on every terminal state (success, failure, disabled). Two
    guards prevent unwanted advances:
      - If `check_disabled_flag` XCom is False (integration disabled),
        skip the advance so re-enabling catches up the missed window.
      - If `prepare_sync_timestamps` didn't produce a timestamps dict
        (task skipped or failed early), skip the advance.
    """
    try:
        is_enabled = rail.result('check_disabled_flag')
    except KeyError:
        is_enabled = True
    if not is_enabled:
        print("Integration disabled; skipping watermark advance")
        return None

    try:
        timestamps = rail.result('prepare_sync_timestamps')
    except KeyError:
        timestamps = None
    if not isinstance(timestamps, dict) or not timestamps.get(
        'current_sync_time'
    ):
        print(
            "prepare_sync_timestamps did not produce a current_sync_time "
            "(skipped or failed); leaving watermark Variable unchanged."
        )
        return None

    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = _watermark_variable_key(instance, customer_id)
    current_time = timestamps['current_sync_time']
    Variable.set(key, current_time)
    print(f"Updated last sync time Variable '{key}' to: {current_time}")
    return current_time


# ---------------------------------------------------------------------------
# Disabled-flag check (replaces Workato 014_503_PSA.CFG_DisableTimesheetIntegration
# account property). Returns True when the integration is ENABLED.
# ---------------------------------------------------------------------------
def is_integration_enabled_method(instance):
    """True when CFG_DisableTimesheetIntegration_{instance} is not 'true'."""
    flag = Variable.get(
        f'CFG_DisableTimesheetIntegration_{instance}',
        default_var='false'
    )
    enabled = str(flag).strip().lower() != 'true'
    if not enabled:
        print(f"Timesheets integration disabled for instance '{instance}' "
              f"via CFG_DisableTimesheetIntegration_{instance}")
    return enabled


# ---------------------------------------------------------------------------
# PSA Ledger filtering (replaces Workato `Desc1 not_contains 'Labor Posting -
# Billing Transfer'` trigger filter).
# ---------------------------------------------------------------------------
def build_psa_ledger_filter_method():
    """
    OData filter for the polling query. TransType is already encoded
    in the endpoint path (`/PSALedger/TS`) by the operator, so we only
    filter on ModifiedDate here.
    """
    timestamps = rail.result('prepare_sync_timestamps')
    return (
        f"?$filter=ModifiedDate ge datetime'{timestamps['last_sync_time']}'"
        f" and ModifiedDate le datetime'{timestamps['current_sync_time']}'"
    )


def filter_billing_transfer_records_method():
    """Drop ledger rows whose Desc1 contains the billing-transfer marker."""
    records = rail.result('poll_psa_ledger') or []
    if not isinstance(records, list):
        return []
    filtered = [
        r for r in records
        if billing_transfer_marker not in (r.get('Desc1') or '')
    ]
    print(f"Filtered PSA ledger: {len(records)} -> {len(filtered)} "
          f"(after billing-transfer drop)")
    return filtered


# ---------------------------------------------------------------------------
# Lookup tables — backed by the shared mapping_sync S3 collection.
#
# mapping_sync creates per-customer SQLite-in-S3 collections containing
# map_employee and map_firm tables. resolve_employee_method /
# resolve_firm_method query those tables directly with a targeted WHERE
# clause, mirroring the vendor_sync pattern.
# ---------------------------------------------------------------------------
def _mapping_collection_locator():
    """Resolve (integration, customer, integration_type) for the shared
    mapping_sync S3 collection. integration_type is hard-pinned to
    'mapping_sync' so all integrations hit the same S3 object."""
    conf = rail.get_current_context()['dag_run'].conf
    return {
        'integration': s3_integration_name,
        'customer': conf.get('customerId'),
        'integration_type': s3_mapping_integration_type,
    }


def _query_mapping_row(task_id, query, query_params, table_name):
    """Execute a single-row S3 collection query against the shared mapping_sync
    collection. Returns the raw row (dict or tuple) on hit, None on miss, and
    treats FileNotFoundError / 'no such table' as not-found so callers always
    fall through to the missing-mapping error path when mapping_sync hasn't
    populated this customer's collection yet."""
    context = rail.get_current_context()
    locator = _mapping_collection_locator()
    query_op = rail.S3QueryCollectionOperator(
        task_id=task_id,
        query=query,
        query_params=query_params,
        integration=locator['integration'],
        customer=locator['customer'],
        integration_type=locator['integration_type'],
        mode='single-row',
    )
    try:
        return query_op.execute(context)
    except FileNotFoundError:
        logger.warning(
            "%s lookup: no S3 collection for customer '%s'; treating as not-found.",
            table_name, locator['customer'],
        )
        return None
    except Exception as exc:
        if 'no such table' in str(exc).lower():
            logger.warning(
                "%s lookup: table not present for customer '%s'; treating as not-found.",
                table_name, locator['customer'],
            )
            return None
        raise


def _fetched_record():
    """
    Unwrap the single-element list returned by VantagepointPsaledgerOperator
    when fetching by composite key (PostSeq, Period). Returns {} when empty.
    """
    result = rail.result('fetch_record_detail') or []
    if isinstance(result, list):
        return result[0] if result else {}
    return result or {}


def resolve_employee_method():
    """
    Find the employee mapping row for PSA Ledger 'Employee' via S3 collection.
    Mirrors recipe step 5 (lookup_table search by col1=Employee) and step 7
    (set QBOEmployeeID = entry.col2).
    """
    record = _fetched_record()
    vp_employee = str(record.get('Employee') or '').strip()
    if not vp_employee:
        return None
    row = _query_mapping_row(
        task_id='_lookup_employee_mapping',
        query=(
            f'SELECT Employee, QBOID, QBOVendorID, QBOVendorName, Name '
            f'FROM {map_employee_table_name} WHERE Employee = ? LIMIT 1'
        ),
        query_params=[vp_employee],
        table_name=map_employee_table_name,
    )
    if not row:
        return None
    if isinstance(row, dict):
        return {
            'QBOEmployeeID': row.get('QBOID') or '',
            'QBOVendorID': row.get('QBOVendorID') or '',
            'QBOVendorName': row.get('QBOVendorName') or '',
            'Name': row.get('Name') or '',
        }
    try:
        return {
            'QBOEmployeeID': row[1] or '',
            'QBOVendorID': row[2] or '',
            'QBOVendorName': row[3] or '',
            'Name': row[4] or '',
        }
    except (TypeError, IndexError):
        return None


def resolve_firm_method():
    """
    Find the firm mapping row for PSA Ledger 'ProjectClientID' via S3 collection.
    Mirrors recipe steps 11-13 (lookup_table search by col1=FirmID, set
    QBOCustomerID = entry.col2; vendor branch via 'Is Vendor' flag).
    """
    record = _fetched_record()
    vp_firm_id = str(record.get('ProjectClientID') or '').strip()
    if not vp_firm_id:
        return None
    row = _query_mapping_row(
        task_id='_lookup_firm_mapping',
        query=(
            f'SELECT FirmID, QBOID, IsVendor, Name '
            f'FROM {map_firm_table_name} WHERE FirmID = ? LIMIT 1'
        ),
        query_params=[vp_firm_id],
        table_name=map_firm_table_name,
    )
    if not row:
        return None
    if isinstance(row, dict):
        return {
            'QBOCustomerID': row.get('QBOID') or '',
            'IsVendor': str(row.get('IsVendor') or 'N').upper() == 'Y',
            'Name': row.get('Name') or '',
        }
    try:
        return {
            'QBOCustomerID': row[1] or '',
            'IsVendor': str(row[2] or 'N').upper() == 'Y',
            'Name': row[3] or '',
        }
    except (TypeError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Validation gates used by the router DAG IfOperators.
# ---------------------------------------------------------------------------
def is_billing_transfer_record_method():
    """Defensive re-check after the full PSA row is fetched."""
    record = _fetched_record()
    return billing_transfer_marker in (record.get('Desc1') or '')


def is_employee_mapping_resolved_method():
    """True when resolve_employee returned a row with QBOEmployeeID."""
    resolved = rail.result('resolve_employee') or {}
    return bool(resolved.get('QBOEmployeeID'))


def is_firm_mapping_resolved_method():
    """True when resolve_firm returned a row with QBOCustomerID."""
    resolved = rail.result('resolve_firm') or {}
    return bool(resolved.get('QBOCustomerID'))


# ---------------------------------------------------------------------------
# Logging stubs (replaces 014-503 PSA Log Message + Send Error Notification
# Email recipes — Airflow provides task logs + email operator natively).
# ---------------------------------------------------------------------------
def log_billing_transfer_skipped():
    record = _fetched_record()
    print(f"Skipping billing-transfer record PostSeq="
          f"{record.get('PostSeq')}, Period={record.get('Period')}")


def log_missing_employee_mapping():
    record = _fetched_record()
    return {
        'error': (
            f"Failed to post employee {record.get('Employee')} timesheet "
            f"(period: {record.get('Period')}, post sequence: "
            f"{record.get('PostSeq')}) to QuickBooks. Details of error: "
            f"No QBO Employee mapping found in 014-503 PSA Map Employee "
            f"lookup table."
        )
    }


def log_missing_firm_mapping():
    record = _fetched_record()
    return {
        'error': (
            f"Failed to post employee {record.get('Employee')} timesheet "
            f"(period: {record.get('Period')}, post sequence: "
            f"{record.get('PostSeq')}) to QuickBooks. Details of error: "
            f"No QBO Customer mapping found in 014-503 PSA Map Firm "
            f"lookup table for ProjectClientID="
            f"{record.get('ProjectClientID')}."
        )
    }


# ---------------------------------------------------------------------------
# QBO TimeActivity payload builders. The recipe creates up to 3 TimeActivities
# per ledger row: regular hours, overtime hours, special overtime hours.
# ---------------------------------------------------------------------------
def _format_qbo_date(value):
    """PSA TransDate -> QBO TxnDate ('YYYY-MM-DD')."""
    if not value:
        return None
    if isinstance(value, str):
        if 'T' in value:
            return value.split('T')[0]
        return value[:10]
    return value


def _split_hours_minutes(decimal_hours):
    """
    Split decimal hours into (Hours, Minutes) integers for QBO.
    Rounds total minutes first to avoid `Minutes == 60` overflow
    (e.g. 0.999 hours -> would be (0, 60); now (1, 0)).
    """
    if decimal_hours is None:
        return 0, 0
    total_minutes = int(round(float(decimal_hours) * 60))
    return divmod(total_minutes, 60)


# pylint: disable=too-many-arguments
def _build_time_activity_body(
    record, employee_id, customer_id, hours_value, rate, description_prefix
):
    hours, minutes = _split_hours_minutes(hours_value)
    body = {
        'NameOf': 'Employee',
        'EmployeeRef': {'value': str(employee_id)},
        'CustomerRef': {'value': str(customer_id)},
        'TxnDate': _format_qbo_date(record.get('TransDate')),
        'Hours': hours,
        'Minutes': minutes,
        'HourlyRate': rate,
        'ItemRef': {
            'value': str(record.get('LaborCode') or '').strip()
        },
        'Description': (
            f"{description_prefix} for {record.get('Name') or ''} - "
            f"{_format_qbo_date(record.get('TransDate'))}"
        ),
        'PrivateNote': (
            f"vp_psa:PostSeq={record.get('PostSeq')};"
            f"Period={record.get('Period')};"
            f"Type={description_prefix}"
        ),
    }
    # BillableStatus requires QBO Time-Tracking AND Billable-Time to be
    # enabled on the realm. When off, QBO rejects the TimeActivity post
    # with a ValidationFault. Gate behind a per-tenant capability flag.
    if _qbo_capability_enabled('time_tracking'):
        body['BillableStatus'] = (
            'Billable'
            if str(record.get('BillStatus') or '').upper() == 'B'
            else 'NotBillable'
        )
    return body


def _resolved_qbo_ids():
    """Pull resolved QBO ids out of upstream task xcom."""
    employee = rail.result('resolve_employee') or {}
    firm = rail.result('resolve_firm') or {}
    return employee.get('QBOEmployeeID'), firm.get('QBOCustomerID')


def build_regular_time_activity_body():
    record = _fetched_record()
    employee_id, customer_id = _resolved_qbo_ids()
    return _build_time_activity_body(
        record=record,
        employee_id=employee_id,
        customer_id=customer_id,
        hours_value=record.get('RegHrs'),
        rate=record.get('Rate'),
        description_prefix='Time entry',
    )


def build_overtime_time_activity_body():
    record = _fetched_record()
    employee_id, customer_id = _resolved_qbo_ids()
    return _build_time_activity_body(
        record=record,
        employee_id=employee_id,
        customer_id=customer_id,
        hours_value=record.get('OvtHrs'),
        rate=record.get('OvtRate'),
        description_prefix='Overtime entry',
    )


def build_special_overtime_time_activity_body():
    record = _fetched_record()
    employee_id, customer_id = _resolved_qbo_ids()
    return _build_time_activity_body(
        record=record,
        employee_id=employee_id,
        customer_id=customer_id,
        hours_value=record.get('SpecialOvtHrs'),
        rate=record.get('SpecialOvtRate'),
        description_prefix='Special overtime entry',
    )


def has_overtime_hours_method():
    record = _fetched_record()
    return float(record.get('OvtHrs') or 0) > 0


def has_special_overtime_hours_method():
    record = _fetched_record()
    return float(record.get('SpecialOvtHrs') or 0) > 0


# ---------------------------------------------------------------------------
# Error capture (replaces the catch{} block in the post recipe and
# 014-503 PSA Send Error Notification Email recipe).
# ---------------------------------------------------------------------------
def capture_create_dag_error(post_seq, period, fallback_error_message):
    """
    Catch task for time_activity_create_dag (the merged router + create).
    Aggregates: validation errors logged by `log_missing_*_mapping_action`,
    skipped billing-transfer notes, and any QBO post failure passed via
    `get_error_message()`. Always RETURNS (never raises) so the
    dispatcher's WaitForDagRunsSensor sees this dag run as SUCCESS and
    `gather_create_dag_errors` can collect the dict.
    """
    child_errors = []

    for log_task in (
        'log_missing_employee_mapping_action',
        'log_missing_firm_mapping_action',
    ):
        try:
            err = rail.result(log_task)
            if err:
                child_errors.append(err)
        except Exception:
            pass

    if child_errors:
        error_message = ' | '.join(
            e.get('error', str(e)) for e in child_errors if e
        )
    elif fallback_error_message:
        error_message = (
            f"PostSeq {post_seq}, Period {period} - "
            f"create TimeActivity failed: {fallback_error_message}"
        )
    else:
        return None

    return {
        'error': error_message,
        'PostSeq': post_seq,
        'Period': period,
    }
