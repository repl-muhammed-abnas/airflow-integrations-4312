"""
Common utility methods for VP QBO Employee Sync integration.
"""
import logging
from datetime import date, datetime

import pycountry
import rail
# IntegrationConfig is used for CFG_* defaults (get_cfg). The shared collection
# helpers + table/column constants come from common so the S3 access logic and
# SQLite identifiers can't drift across integrations.
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    collection_update,
    collection_upsert,
    read_lookup_variable,
)
from vp_quickbooks_integration.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_COLUMNS,
    MAP_EMPLOYEE_UNIQUE_COLUMNS,
)

logger = logging.getLogger(__name__)


def lookup_default_labor_type(instance):
    """Default `Type` (labor type) for new VP employees.

    Ports Workato account property `014_503_PSA_CFG_DefaultEmployeeLaborType`.
    Resolved CFG-first from the middleware integration payload
    (`dag_run.conf['config']['CFG_DefaultEmployeeLaborType']`, via
    IntegrationConfig.get_cfg), falling back to the legacy per-instance Variable
    `vp_qbo_employee_sync_default_labor_type_{instance}` for backwards
    compatibility. Callers apply a final `or 'E'` default.
    """
    try:
        context = rail.get_current_context()
    except Exception:  # pylint: disable=broad-exception-caught
        context = None
    if context is not None:
        value = IntegrationConfig.get_cfg(
            context, 'CFG_DefaultEmployeeLaborType'
        )
        if value:
            return value
    return read_lookup_variable(
        f'vp_qbo_employee_sync_default_labor_type_{instance}',
        default='E'
    )


def lookup_default_org(instance):
    """Default Org for new/updated VP employees when QBO payload has none.

    Reads per-instance Variable `vp_qbo_employee_sync_default_org_{instance}`.
    """
    return read_lookup_variable(
        f'vp_qbo_employee_sync_default_org_{instance}',
        default=None
    )


# Collection access (read/write the shared map_employee collection) uses the
# shared helpers in common.python_callable_method — `collection_rows` /
# `collection_update`, imported above and called directly.


# ---------------------------------------------------------------------------
# Employee map (Workato `014-503 PSA Map Employee` lookup) — read/written as the
# shared mapping_sync `map_employee` collection (NOT an Airflow Variable).
# Columns come from common.tables.MAP_EMPLOYEE_COLUMNS:
#   Employee, QBOID, QBOVendorID, QBOVendorName, Name
# QBOVendorID/QBOVendorName are populated by a separate Workato vendor-link flow
# not yet ported — held as empty strings here for schema parity. map_employee
# carries a UNIQUE index on QBOID (MAP_EMPLOYEE_UNIQUE_COLUMNS), so writes use a
# single atomic upsert keyed on QBOID rather than DELETE-then-INSERT.
# ---------------------------------------------------------------------------


def _write_map_employee_row(employee, qbo_id, qbo_vendor_id, qbo_vendor_name,
                            name, context=None):
    """Upsert one map_employee row keyed by QBOID via a single atomic
    S3UpsertCollectionOperator call.

    Keyed on MAP_EMPLOYEE_UNIQUE_COLUMNS (QBOID): ``INSERT ... ON CONFLICT(QBOID)
    DO UPDATE`` replaces the existing row's non-key columns in one S3 commit
    (dup-free, idempotent, atomic) — supersedes the old DELETE-then-INSERT, which
    was only needed before map_employee gained its UNIQUE index. A missing
    collection / table raises naturally (the employee is already live in VP, so
    we must not silently skip the map write — the next sync would otherwise
    create a duplicate). Column ordering is driven by
    common.tables.MAP_EMPLOYEE_COLUMNS so SQLite identifiers can't drift from
    what mapping_sync created.
    """
    context = context or rail.get_current_context()
    values = {
        'Employee': employee,
        'QBOID': str(qbo_id),
        'QBOVendorID': qbo_vendor_id or '',
        'QBOVendorName': qbo_vendor_name or '',
        'Name': name or '',
    }
    collection_upsert(
        MAP_EMPLOYEE_TABLE_NAME,
        MAP_EMPLOYEE_UNIQUE_COLUMNS,
        {col: values[col] for col in MAP_EMPLOYEE_COLUMNS},
        context,
    )


def _employee_record_from_create_response():
    """Extract the employee record from create_employee_in_vp XCom."""
    response = rail.result('create_employee_in_vp')
    if isinstance(response, list) and response:
        return response[0] or {}
    if isinstance(response, dict):
        return response
    return {}


def _employee_record_from_update_response():
    """Extract the employee record from update_employee_in_vp XCom."""
    response = rail.result('update_employee_in_vp')
    if isinstance(response, list) and response:
        return response[0] or {}
    if isinstance(response, dict):
        return response
    return {}


def capture_employee_id_from_create():
    """Pull VP-assigned Employee code from create_employee_in_vp response."""
    record = _employee_record_from_create_response()
    return record.get('Employee')


def add_employee_to_employee_map():
    """Insert/update the employee map row after a successful create.

    Mirrors the Workato 'insert_entry on 014-503 PSA Map Employee' step.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    vp_employee = capture_employee_id_from_create()

    if not qbo_id or not vp_employee:
        logger.warning(
            "Skipping employee map write — missing key field "
            "(qbo_id=%s, vp_employee=%s)", qbo_id, vp_employee
        )
        return None

    response_record = _employee_record_from_create_response()
    _write_map_employee_row(
        vp_employee,
        qbo_id,
        response_record.get('QBOVendorID'),
        response_record.get('QBOVendorName'),
        conf.get('DisplayName'),
    )
    logger.info(
        "Added map_employee entry: QBOID %s -> Employee %s",
        qbo_id, vp_employee
    )
    return vp_employee


def refresh_employee_in_employee_map():
    """Refresh the lookup row after a successful update.

    Mirrors the Workato 'update_entry on 014-503 PSA Map Employee' step on
    the update branch — keeps `Name` (and vendor-link fields, if VP returns
    them) in sync with QBO/VP after employee changes. `Employee` and
    `QBOID` are never touched (they don't change post-creation).
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    if not qbo_id:
        logger.warning("Skipping employee map refresh — missing QBO Id")
        return None

    existing = lookup_employee_by_qboid()
    if not existing:
        logger.info(
            "Skipping employee map refresh — no existing row for QBOID %s",
            qbo_id
        )
        return None

    response_record = _employee_record_from_update_response()
    name = conf.get('DisplayName') or existing.get('Name') or ''
    qbo_vendor_id = existing.get('QBOVendorID') or ''
    qbo_vendor_name = existing.get('QBOVendorName') or ''
    if 'QBOVendorID' in response_record:
        qbo_vendor_id = response_record.get('QBOVendorID') or ''
    if 'QBOVendorName' in response_record:
        qbo_vendor_name = response_record.get('QBOVendorName') or ''

    rowid = existing.get('_rowid')
    if rowid is None:
        # No rowid (shouldn't happen via collection_rows) — fall back to the
        # DELETE-then-INSERT upsert keyed by QBOID.
        _write_map_employee_row(
            existing.get('Employee'), qbo_id,
            qbo_vendor_id, qbo_vendor_name, name
        )
    else:
        # In-place update by rowid — single atomic statement (no DELETE/INSERT
        # window), keeping Employee/QBOID untouched.
        collection_update(
            MAP_EMPLOYEE_TABLE_NAME,
            f"UPDATE {MAP_EMPLOYEE_TABLE_NAME} "
            "SET Name = ?, QBOVendorID = ?, QBOVendorName = ? WHERE rowid = ?",
            [name, qbo_vendor_id, qbo_vendor_name, rowid],
        )
    logger.info(
        "Refreshed map_employee entry: QBOID %s -> Name '%s'", qbo_id, name
    )
    return existing


# ---------------------------------------------------------------------------
# Router (router_dag) helpers
# ---------------------------------------------------------------------------

def lookup_employee_by_qboid():
    """Find an existing employee row in the shared map_employee collection by
    QBOID.

    Ports the Workato 'PSA Employee QBOID Exists' lookup to the mapping_sync S3
    collection. Returns the row dict {Employee, QBOID, QBOVendorID,
    QBOVendorName, Name, _rowid} or None. A missing collection / table (e.g. a
    brand-new customer mapping_sync hasn't populated) is treated as 'not found',
    so the router falls through to the create path.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    if not qbo_id:
        return None
    rows = collection_rows(
        MAP_EMPLOYEE_TABLE_NAME,
        MAP_EMPLOYEE_COLUMNS,
        "QBOID = ?",
        [str(qbo_id)],
    )
    return rows[0] if rows else None


def check_employee_exists_in_lookup():
    """IfOperator test: did get_employee_from_lookup return a row?"""
    return rail.result('get_employee_from_lookup') is not None


def build_employee_conf(operation_type):
    """Build conf for the employee_create / employee_update child DAG."""
    conf = rail.get_current_context()['dag_run'].conf
    result = {
        **conf,
        'type': operation_type,
        'connections': conf.get('connections')
    }
    if operation_type == 'update':
        employee_row = rail.result('get_employee_from_lookup')
        if employee_row:
            result['vp_employee_id'] = employee_row.get('Employee')
    return result


def collect_triggered_dagrun_ids():
    """Collect dag run(s) from whichever trigger executed (create or update)."""
    dag_runs = []
    for task_id in ['trigger_employee_create', 'trigger_employee_update']:
        try:
            result = rail.result(task_id)
            if result is not None:
                dag_runs.append(result)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    return dag_runs


# ---------------------------------------------------------------------------
# Format / conversion helpers
# ---------------------------------------------------------------------------

def format_date_to_yyyy_mm_dd(date_value):
    """Coerce a date-like input to a `YYYY-MM-DD` string.

    Accepts:
    - ISO 8601 string ('2026-05-18T10:28:25Z') — strips the time portion
    - `datetime.datetime` — emits the calendar date
    - `datetime.date` — emits ISO form

    `datetime` is checked before `date` because `datetime` is a subclass
    of `date` and `datetime.isoformat()` would otherwise include the time.

    Returns None when input is falsy or of an unrecognised type.
    """
    if not date_value:
        return None
    if isinstance(date_value, str):
        return date_value.split('T')[0]
    if isinstance(date_value, datetime):
        return date_value.date().isoformat()
    if isinstance(date_value, date):
        return date_value.isoformat()
    return None


def get_country_code(country_value):
    """Free-form country name -> ISO 3166-1 alpha-2. None if no match.

    Uses pycountry fuzzy search so values like 'USA', 'United States',
    'United States of America' all resolve to 'US'.
    """
    if not country_value:
        return None
    try:
        matches = pycountry.countries.search_fuzzy(country_value)
        if matches:
            return matches[0].alpha_2
    except LookupError:
        return None
    return None


def _qbo_status_to_vp_employee(active):
    """QBO Active (bool) -> VP Employee Status code ('A' or 'T')."""
    if active is True:
        return 'A'
    if active is False:
        return 'T'
    return None


def _filter_none_and_empty(body):
    """Filter out None and empty-string values.

    vendor_sync filters only None, but VP rejects empty strings on some
    Employee fields (Org, Country, Status). Extended here to drop both.
    """
    return {k: v for k, v in body.items() if v is not None and v != ''}


# ---------------------------------------------------------------------------
# Title (CFGEmployeeTitle) pre-flight helpers
# ---------------------------------------------------------------------------

def has_title_input():
    """IfOperator test: did QBO supply a Title field?"""
    conf = rail.get_current_context()['dag_run'].conf
    return bool(conf.get('Title'))


def _unwrap_settings_response(raw):
    """Normalize VantagepointSettingsListOperator response to a list."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ('rows', 'Body', 'body', 'array', 'data'):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def check_job_title_match():
    """IfOperator test: does QBO Title already exist in CFGEmployeeTitle?

    Case-insensitive match against the `Code` column. Returns True if a
    matching code-table entry is found.
    """
    title_codes = _unwrap_settings_response(
        rail.result('get_job_titles_from_vp')
    )
    target = (
        rail.get_current_context()['dag_run'].conf.get('Title') or ''
    ).strip().upper()
    if not target:
        return False
    return any(
        isinstance(entry, dict)
        and (entry.get('Code') or '').strip().upper() == target
        for entry in title_codes
    )


def build_create_job_title_body():
    """POST /codeTable/CFGEmployeeTitle: add the new title code."""
    title = rail.get_current_context()['dag_run'].conf.get('Title')
    return {'Code': title, 'Description': title}


# ---------------------------------------------------------------------------
# Employee body builders
# ---------------------------------------------------------------------------

def _qbo_address_inputs():
    """Pull QBO PrimaryAddr fields from conf into a flat dict."""
    conf = rail.get_current_context()['dag_run'].conf
    primary_addr = conf.get('PrimaryAddr') or {}
    return {
        'Address1': primary_addr.get('Line1'),
        'Address2': primary_addr.get('Line2'),
        'Address3': primary_addr.get('Line3'),
        'City': primary_addr.get('City'),
        'State': primary_addr.get('CountrySubDivisionCode'),
        'ZIP': primary_addr.get('PostalCode'),
        'Country': get_country_code(primary_addr.get('Country'))
    }


def _qbo_contact_inputs():
    """Pull QBO contact fields (email/phone) from conf into a flat dict."""
    conf = rail.get_current_context()['dag_run'].conf
    primary_phone = conf.get('PrimaryPhone') or {}
    mobile = conf.get('Mobile') or {}
    primary_email = conf.get('PrimaryEmailAddr') or {}
    return {
        'EMail': primary_email.get('Address'),
        'HomePhone': primary_phone.get('FreeFormNumber'),
        'MobilePhone': mobile.get('FreeFormNumber')
    }


def get_first_vp_organization():
    """Read the first Org code from the get_vp_organizations task result.

    Mirrors the Workato upsert recipe's `first VP organization` fallback —
    when neither the per-instance default Variable nor QBO `Organization`
    yields a value, use the first row from VP `GET /organization`.

    Returns None if the task didn't run, returned no rows, or the first
    row has no `Org` field.
    """
    try:
        rows = _unwrap_settings_response(rail.result('get_vp_organizations'))
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    if not rows or not isinstance(rows[0], dict):
        return None
    return rows[0].get('Org')


def build_create_employee_body(instance):
    """Body for POST /employee (new employee).

    Shape mirrors the Workato `014_503_psa_vantagepoint_upsert_employee`
    create branch and the verified Postman happy-path:
    - `Employee = '[AUTONUMBER]'` sentinel — VP assigns the code, read back
      from response via `capture_employee_id_from_create`.
    - `QBOID` stores the QBO Employee Id for downstream matching.
    - `Salutation` (VP field) maps from QBO `Title`.
    - `Type` is the Employee type code (e.g. 'E'); ports the Workato
      account property `014_503_PSA_CFG_DefaultEmployeeLaborType` via
      Variable `vp_qbo_employee_sync_default_labor_type_{instance}`.
    - `Status` derives from QBO.Active: True→'A', False→'T'.
    - `Org` falls back to Variable `vp_qbo_employee_sync_default_org_{instance}`
      then QBO `Organization`.
    - Empty strings are sent explicitly to match the Postman happy-path —
      VP rejects missing required fields but accepts blank values.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    address = _qbo_address_inputs()
    contact = _qbo_contact_inputs()
    active = conf.get('Active')
    termination_date = (
        format_date_to_yyyy_mm_dd(conf.get('ReleasedDate'))
        if active is False else None
    )

    return {
        'Employee': '[AUTONUMBER]',
        'QBOID': qbo_id or '',
        'FirstName': conf.get('GivenName') or '',
        'LastName': conf.get('FamilyName') or '',
        'Salutation': conf.get('Title') or '',
        'Suffix': conf.get('Suffix') or '',
        'EMail': contact['EMail'] or '',
        'HomePhone': contact['HomePhone'] or '',
        'MobilePhone': contact['MobilePhone'] or '',
        'Address1': address['Address1'] or '',
        'Address2': address['Address2'] or '',
        'Address3': address['Address3'] or '',
        'City': address['City'] or '',
        'State': address['State'] or '',
        'ZIP': address['ZIP'] or '',
        'Country': address['Country'] or '',
        'HireDate': format_date_to_yyyy_mm_dd(conf.get('HiredDate')) or '',
        'TerminationDate': termination_date or '',
        'Status': _qbo_status_to_vp_employee(active) or 'A',
        'Org': (
            lookup_default_org(instance)
            or conf.get('Organization')
            or get_first_vp_organization()
            or ''
        ),
        'OrganizationName': conf.get('Organization') or '',
        'HomeCompany': conf.get('HomeCompany') or '',
        'EmployeeCompany': conf.get('EmployeeCompany') or '',
        'Type': lookup_default_labor_type(instance) or 'E',
        'ReadyForProcessing': 'true',
        'ReadyForApproval': 'true',
    }


def build_update_employee_body(instance):
    """Body for PUT /employee/{vp_employee_id} (update existing employee).

    Differs from create body:
    - Omits `Employee` (passed via URL path from `conf.vp_employee_id`).
    - **Empty-string fields are dropped** so VP preserves the existing values
      (PUT with `""` on required fields like `HomeCompany`/`EmployeeCompany`
      is treated as a clear and rejected by VP validation). Create body
      keeps empties because VP auto-defaults them on insert.
    - `TerminationDate` is always populated: empty string when QBO.Active is
      True (explicit clear, supports rehire), date string when QBO.Active is
      False. Set AFTER the empty-string drop so the clear signal survives.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    address = _qbo_address_inputs()
    contact = _qbo_contact_inputs()
    active = conf.get('Active')

    body = {
        'QBOID': qbo_id or '',
        'FirstName': conf.get('GivenName') or '',
        'LastName': conf.get('FamilyName') or '',
        'Salutation': conf.get('Title') or '',
        'Suffix': conf.get('Suffix') or '',
        'EMail': contact['EMail'] or '',
        'HomePhone': contact['HomePhone'] or '',
        'MobilePhone': contact['MobilePhone'] or '',
        'Address1': address['Address1'] or '',
        'Address2': address['Address2'] or '',
        'Address3': address['Address3'] or '',
        'City': address['City'] or '',
        'State': address['State'] or '',
        'ZIP': address['ZIP'] or '',
        'Country': address['Country'] or '',
        'HireDate': format_date_to_yyyy_mm_dd(conf.get('HiredDate')) or '',
        'Status': _qbo_status_to_vp_employee(active) or 'A',
        'Org': (
            lookup_default_org(instance)
            or conf.get('Organization')
            or get_first_vp_organization()
            or ''
        ),
        'OrganizationName': conf.get('Organization') or '',
        'HomeCompany': conf.get('HomeCompany') or '',
        'EmployeeCompany': conf.get('EmployeeCompany') or '',
        'Type': lookup_default_labor_type(instance) or 'E',
        'ReadyForProcessing': 'true',
        'ReadyForApproval': 'true',
    }

    body = {k: v for k, v in body.items() if v != ''}

    if active is False:
        body['TerminationDate'] = (
            format_date_to_yyyy_mm_dd(conf.get('ReleasedDate')) or ''
        )
    else:
        body['TerminationDate'] = ''

    return body


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps DAG SUCCESS so parent
# WaitForDagRunsSensor never sees a failed run)
# ---------------------------------------------------------------------------

def _format_employee_label(qbo_employee_id, display_name):
    """Format the employee identifier prefix for error messages.

    If display_name is present, output looks like `Employee 56 (Jane Smith)`.
    Otherwise just `Employee 56`.
    """
    if display_name and str(display_name).strip():
        return f"Employee {qbo_employee_id} ({str(display_name).strip()})"
    return f"Employee {qbo_employee_id}"


def capture_create_error(qbo_employee_id, display_name, error_message):
    return {
        'error': (
            f"{_format_employee_label(qbo_employee_id, display_name)} - "
            f"create failed: {error_message}"
        )
    }


def capture_update_error(qbo_employee_id, display_name, error_message):
    return {
        'error': (
            f"{_format_employee_label(qbo_employee_id, display_name)} - "
            f"update failed: {error_message}"
        )
    }


def capture_router_dag_error(
    qbo_employee_id, display_name, fallback_error_message
):
    """Aggregate child errors; fall back to local message; return dict or None.

    Mirrors vendor_sync's capture_router_dag_error pattern.
    """
    child_errors = []
    try:
        gathered = rail.result('gather_employee_dag_errors')
        if gathered:
            child_errors = (
                gathered if isinstance(gathered, list) else [gathered]
            )
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    if child_errors:
        error_message = ' | '.join(
            e.get('error', str(e)) for e in child_errors if e
        )
    elif fallback_error_message:
        error_message = (
            f"{_format_employee_label(qbo_employee_id, display_name)} - "
            f"sync failed: {fallback_error_message}"
        )
    else:
        return None

    return {'error': error_message}


# Watermark helpers (sanitize_customer_id, build_watermark_variable_key,
# utc_now_iso, prepare_sync_timestamps, update_last_sync_time) now live in
# common.python_callable_method; the dispatcher imports them from there.
