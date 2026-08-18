"""Common utility methods for VP -> Xero Employee Sync Upsert."""

# pylint: disable=invalid-name,broad-exception-caught
import logging
from urllib.parse import quote
import rail
from vp_xero_integration.common.python_callable_method import (
    collection_rows,
    collection_update,
    collection_upsert,
    unwrap_vp_response,
)
from vp_xero_integration.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_COLUMNS,
    MAP_EMPLOYEE_UNIQUE_COLUMNS,
)

logger = logging.getLogger(__name__)


def build_vp_employee_filter_method():
    timestamps = rail.result('prepare_sync_timestamps')
    last = quote(timestamps['last_sync_time'], safe='')
    current = quote(timestamps['current_sync_time'], safe='')
    gte = quote('>=', safe='')
    lt = quote('<', safe='')
    return (
        f"?filterHash[0][name]=ModDate"
        f"&filterHash[0][value]={last}"
        f"&filterHash[0][type]=datetime"
        f"&filterHash[0][opp]={gte}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=ModDate"
        f"&filterHash[1][value]={current}"
        f"&filterHash[1][type]=datetime"
        f"&filterHash[1][opp]={lt}"
        f"&filterHash[1][seq]=1"
    )


def build_vp_single_employee_filter_method(**context):
    """Recipe step 3: filter the VP LIST endpoint by Employee==<code>.
    """
    code = quote((context['dag_run'].conf.get('Employee') or '').strip(), safe='')
    eq = quote('==', safe='')
    return (
        f"?filterHash[0][name]=Employee"
        f"&filterHash[0][value]={code}"
        f"&filterHash[0][type]=string"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][seq]=0"
    )


def extract_employee_list_method():
    raw = rail.result('get_changed_employees_from_vp')
    records = unwrap_vp_response(raw, strict=True)
    employees = [
        {
            'Employee': r.get('Employee'),
            'ModDate': r.get('ModDate'),
            'Name': _full_name(r),
        }
        for r in records
        if isinstance(r, dict) and r.get('Employee')
    ]
    logger.info(
        "Found %d changed VP employees in this window", len(employees)
    )
    return employees


def check_if_employees_exist_method():
    return len(rail.result('extract_employee_list') or []) > 0


def _write_map_employee_row(employee, contact_id, status, account_number,
                            created_date, mod_date, messages, context=None):
    context = context or rail.get_current_context()
    values = {
        'Employee': employee,
        'ContactID': contact_id or '',
        'Status': status or '',
        'AccountNumber': account_number or '',
        'CreatedDate': created_date or '',
        'ModDate': mod_date or '',
        'Messages': messages or '',
    }
    collection_upsert(
        MAP_EMPLOYEE_TABLE_NAME,
        MAP_EMPLOYEE_UNIQUE_COLUMNS,
        {col: values[col] for col in MAP_EMPLOYEE_COLUMNS},
        context,
    )


def _fetched_vp_employee():
    """Unwrap the get_single_employee_from_vp XCom into a single dict."""
    raw = rail.result('get_single_employee_from_vp')
    records = unwrap_vp_response(raw)
    if records:
        first = records[0]
        return first if isinstance(first, dict) else {}
    if isinstance(raw, dict) and raw.get('Employee'):
        return raw
    return {}


def _full_name(record):
    first = (record.get('FirstName') or '').strip()
    last = (record.get('LastName') or '').strip()
    return f"{first} {last}".strip()


def _conf_employee_code():
    """VP Employee code carried from the dispatcher's conf, as a fallback."""
    conf = rail.get_current_context()['dag_run'].conf
    return (conf.get('Employee') or '').strip()


def _conf_mod_date():
    """VP ModDate carried from the dispatcher's conf, as a fallback."""
    conf = rail.get_current_context()['dag_run'].conf
    return (conf.get('ModDate') or '').strip()


def get_employee_from_map_method():
    """Look up map_employee by VP Employee code. Returns row dict or None.
    """
    vp_employee = (_fetched_vp_employee().get('Employee') or '').strip()
    if not vp_employee:
        vp_employee = _conf_employee_code()
    if not vp_employee:
        return None
    rows = collection_rows(
        MAP_EMPLOYEE_TABLE_NAME,
        MAP_EMPLOYEE_COLUMNS,
        "Employee = ?",
        [vp_employee],
    )
    if not rows:
        return None
    with_contact = [r for r in rows if (r.get('ContactID') or '').strip()]
    return with_contact[0] if with_contact else rows[0]


def check_employee_exists_in_map_method():
    """IfOperator test: did get_employee_from_map return a row?"""
    return rail.result('get_employee_from_map') is not None


def update_employee_in_map_method():
    record = _fetched_vp_employee()
    vp_employee = (
        (record.get('Employee') or '').strip()
        or _conf_employee_code()
    )
    if not vp_employee:
        logger.warning("Skipping map update — no VP Employee code available")
        return None

    existing = rail.result('get_employee_from_map') or {}
    status = (record.get('Status') or existing.get('Status') or '').strip()
    mod_date = (
        (record.get('ModDate') or '').strip()
        or _conf_mod_date()
        or existing.get('ModDate')
        or ''
    )

    rowid = existing.get('_rowid')
    if rowid is None:
        logger.error(
            "Skipping map update for Employee=%s — existing row has no "
            "_rowid; re-run will retry.", vp_employee
        )
        return None
    collection_update(
        MAP_EMPLOYEE_TABLE_NAME,
        f"UPDATE {MAP_EMPLOYEE_TABLE_NAME} "
        "SET Status = ?, ModDate = ? WHERE rowid = ?",
        [status, mod_date, rowid],
    )
    logger.info(
        "Updated map_employee row: Employee=%s Status='%s' ModDate='%s'",
        vp_employee, status, mod_date
    )
    return {
        'Employee': vp_employee,
        'ContactID': existing.get('ContactID') or '',
        'Status': status,
        'AccountNumber': existing.get('AccountNumber') or '',
        'CreatedDate': existing.get('CreatedDate') or '',
        'ModDate': mod_date,
        'Messages': existing.get('Messages') or '',
    }


def add_employee_to_map_method():
    record = _fetched_vp_employee()
    vp_employee = (
        (record.get('Employee') or '').strip()
        or _conf_employee_code()
    )
    if not vp_employee:
        logger.warning("Skipping map add — no VP Employee code available")
        return None

    status = (record.get('Status') or '').strip()
    mod_date = (record.get('ModDate') or '').strip() or _conf_mod_date()

    _write_map_employee_row(vp_employee, '', status, '', '', mod_date, '')
    logger.info(
        "Added map_employee row: Employee=%s Status='%s' ModDate='%s'",
        vp_employee, status, mod_date
    )
    return {
        'Employee': vp_employee,
        'ContactID': '',
        'Status': status,
        'AccountNumber': '',
        'CreatedDate': '',
        'ModDate': mod_date,
        'Messages': '',
    }
    
def _format_employee_label(vp_employee_code, full_name):
    if full_name and str(full_name).strip():
        return f"Employee {vp_employee_code} ({str(full_name).strip()})"
    return f"Employee {vp_employee_code}"


def capture_processor_error(vp_employee_code, full_name, error_message):
    """Return an error dict the dispatcher can aggregate."""
    return {
        'error': (
            f"{_format_employee_label(vp_employee_code, full_name)} - "
            f"map sync failed: {error_message}"
        )
    }

def check_vp_returned_employee_method():
    record = _fetched_vp_employee()
    return bool(record and record.get('Employee'))


def should_skip_employee_method():
    record = _fetched_vp_employee()
    terminated = _is_truthy(record.get('Terminated'))
    ready = _is_truthy(record.get('ReadyForApproval'))
    should_skip = terminated or not ready
    if should_skip:
        logger.info(
            "Skipping Employee=%s (Terminated=%r, ReadyForApproval=%r)",
            record.get('Employee'),
            record.get('Terminated'),
            record.get('ReadyForApproval'),
        )
    return should_skip


def _is_truthy(value):
    if value is None:
        return False
    return str(value).strip().lower() in ('y', 'yes', 'true', '1', 't')

def map_row_needs_xero_create_method():
    row = rail.result('get_employee_from_map')
    if not row:
        return True
    return not (row.get('ContactID') or '').strip()


def map_row_is_active_for_update_method():
    row = rail.result('get_employee_from_map') or {}
    return (row.get('Status') or '').strip().upper() == 'ACTIVE'


def map_row_present_for_archive_method():
    row = rail.result('get_employee_from_map') or {}
    return bool((row.get('ContactID') or '').strip())

def _addr_block(record, address_type):
    """Build a Xero Address dict for STREET or POBOX from a VP record."""
    return {
        'AddressType': address_type,
        'AddressLine1': record.get('Address1') or '',
        'AddressLine2': record.get('Address2') or '',
        'AddressLine3': record.get('Address3') or '',
        'City': record.get('City') or '',
        'Region': record.get('State') or '',
        'PostalCode': record.get('ZIP') or '',
        'Country': record.get('Country') or '',
    }


def _phone_block(number, phone_type):
    return {
        'PhoneType': phone_type,
        'PhoneNumber': number or '',
    }


def _common_xero_contact_body(record):
    return {
        'Name': record.get('TitleName') or '',
        'FirstName': record.get('FirstName') or '',
        'LastName': record.get('LastName') or '',
        'EmailAddress': record.get('EMail') or '',
        'AccountNumber': record.get('Employee') or '',
        'Addresses': [
            _addr_block(record, 'STREET'),
            _addr_block(record, 'POBOX'),
        ],
        'Phones': [
            _phone_block(record.get('WorkPhone'), 'DEFAULT'),
            _phone_block(record.get('MobilePhone'), 'MOBILE'),
        ],
    }


def build_xero_create_contact_body_method():
    record = _fetched_vp_employee()
    body = _common_xero_contact_body(record)
    body['ContactStatus'] = 'ACTIVE'
    body['ContactNumber'] = record.get('Employee') or ''
    return body


def build_xero_update_contact_body_method():
    record = _fetched_vp_employee()
    map_row = rail.result('get_employee_from_map') or {}
    body = _common_xero_contact_body(record)
    body['ContactID'] = map_row.get('ContactID') or ''
    body['ContactStatus'] = (
        'ACTIVE'
        if (record.get('Status') or '').strip().upper() == 'A'
        else 'ARCHIVED'
    )
    return body


def build_xero_archive_contact_body_method():
    map_row = rail.result('get_employee_from_map') or {}
    return {
        'ContactID': map_row.get('ContactID') or '',
        'ContactStatus': 'ARCHIVED',
    }

def _xero_first_contact(xcom_value):
    if not xcom_value:
        return {}
    if isinstance(xcom_value, dict):
        # Formatted envelope from XeroBaseOperator._format_xero_response.
        if 'data' in xcom_value and xcom_value.get('success') is not False:
            data = xcom_value.get('data')
            if isinstance(data, list) and data:
                first = data[0]
                return first if isinstance(first, dict) else {}
            if isinstance(data, dict):
                if data.get('ContactID'):
                    return data
                inner = data.get('Contacts')
                if isinstance(inner, list) and inner:
                    first = inner[0]
                    return first if isinstance(first, dict) else {}
        contacts = xcom_value.get('Contacts')
        if isinstance(contacts, list) and contacts:
            first = contacts[0]
            return first if isinstance(first, dict) else {}
        if xcom_value.get('ContactID'):
            return xcom_value
    if isinstance(xcom_value, list) and xcom_value:
        first = xcom_value[0]
        return first if isinstance(first, dict) else {}
    return {}


def _pull_task_error_message(context, task_id):
    ti = context.get('ti') or context.get('task_instance')
    try:
        dag_run = ti.get_dagrun()
        target = next(
            (t for t in dag_run.get_task_instances() if t.task_id == task_id),
            None,
        )
        if target is not None and target.state == 'failed':
            return (
                "Error Upserting Contact in Xero "
                f"(Vantagepoint employee: {_conf_employee_code()})"
            )
    except Exception as exc:  # pylint:disable=broad-exception-caught
        logger.debug("Could not inspect task '%s' state: %s", task_id, exc)
    return ''

def write_map_row_after_create_method(**context):
    """Success: add new map row from Xero response. Failure: log error to map."""
    # No-op when the create branch wasn't taken (update / archive path).
    ti = context.get('ti') or context.get('task_instance')
    try:
        create_state = ti.get_dagrun().get_task_instance(
            'create_contact_in_xero'
        ).state
    except Exception:  # pylint:disable=broad-exception-caught
        create_state = None
    if create_state in ('skipped', 'upstream_failed', 'removed'):
        logger.info(
            "Skipping post-create map write — create_contact_in_xero state=%s",
            create_state,
        )
        return None

    record = _fetched_vp_employee()
    vp_employee = (
        (record.get('Employee') or '').strip() or _conf_employee_code()
    )
    if not vp_employee:
        logger.warning("Skipping post-create map write — no VP Employee code")
        return None

    create_xcom = None
    try:
        create_xcom = rail.result('create_contact_in_xero')
    except Exception:  # pylint:disable=broad-exception-caught
        create_xcom = None
    contact = _xero_first_contact(create_xcom)

    if contact.get('ContactID'):
        _write_map_employee_row(
            employee=vp_employee,
            contact_id=contact.get('ContactID') or '',
            status=contact.get('ContactStatus') or 'ACTIVE',
            account_number=contact.get('AccountNumber') or vp_employee,
            created_date=_now_iso(),
            mod_date=_now_iso(),
            messages='',
            context=context,
        )
        logger.info(
            "map_employee row written after Xero create: Employee=%s "
            "ContactID=%s Status=%s",
            vp_employee, contact.get('ContactID'), contact.get('ContactStatus'),
        )
        return {
            'Employee': vp_employee,
            'ContactID': contact.get('ContactID'),
            'Status': contact.get('ContactStatus') or 'ACTIVE',
            'AccountNumber': contact.get('AccountNumber') or vp_employee,
        }

    error_msg = _pull_task_error_message(context, 'create_contact_in_xero') \
        or (
            "Error Upserting Contact in Xero "
            f"(Vantagepoint employee: {vp_employee})"
        )
    existing = rail.result('get_employee_from_map') or {}
    rowid = existing.get('_rowid')
    if rowid is not None:
        collection_update(
            MAP_EMPLOYEE_TABLE_NAME,
            f"UPDATE {MAP_EMPLOYEE_TABLE_NAME} "
            "SET Messages = ? WHERE rowid = ?",
            [error_msg, rowid],
        )
        logger.warning(
            "Xero create failed for Employee=%s — logged to map.Messages",
            vp_employee,
        )
    else:
        logger.warning(
            "Xero create failed for Employee=%s and no existing map row — "
            " Error: %s",
            vp_employee, error_msg,
        )
    return {'Employee': vp_employee, 'error': error_msg}


def refresh_map_row_after_update_method():
    record = _fetched_vp_employee()
    existing = rail.result('get_employee_from_map') or {}
    rowid = existing.get('_rowid')
    if rowid is None:
        logger.warning(
            "Skipping post-update map refresh — no _rowid on existing row"
        )
        return None
    new_status = (
        'ACTIVE'
        if (record.get('Status') or '').strip().upper() == 'A'
        else 'ARCHIVED'
    )
    collection_update(
        MAP_EMPLOYEE_TABLE_NAME,
        f"UPDATE {MAP_EMPLOYEE_TABLE_NAME} "
        "SET Status = ?, ModDate = ?, Messages = '' WHERE rowid = ?",
        [new_status, _now_iso(), rowid],
    )
    logger.info(
        "map_employee row refreshed after Xero update: Employee=%s "
        "Status=%s",
        existing.get('Employee'), new_status,
    )
    return {'Employee': existing.get('Employee'), 'Status': new_status}


def mark_map_row_archived_method():
    existing = rail.result('get_employee_from_map') or {}
    rowid = existing.get('_rowid')
    if rowid is None:
        logger.warning(
            "Skipping mark-archived — no _rowid on existing map row"
        )
        return None
    collection_update(
        MAP_EMPLOYEE_TABLE_NAME,
        f"UPDATE {MAP_EMPLOYEE_TABLE_NAME} "
        "SET Status = 'ARCHIVED', ModDate = ? WHERE rowid = ?",
        [_now_iso(), rowid],
    )
    logger.info(
        "map_employee row marked ARCHIVED: Employee=%s",
        existing.get('Employee'),
    )
    return {'Employee': existing.get('Employee'), 'Status': 'ARCHIVED'}


def log_result_method():
    conf = rail.get_current_context()['dag_run'].conf
    record = _fetched_vp_employee()
    logger.info(
        "vp_xero_employee_sync result | Employee=%s | Name=%s | "
        "Status=%s | Terminated=%s | ReadyForApproval=%s | customerId=%s",
        record.get('Employee') or conf.get('Employee'),
        _full_name(record) or conf.get('Name'),
        record.get('Status'),
        record.get('Terminated'),
        record.get('ReadyForApproval'),
        conf.get('customerId'),
    )


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
