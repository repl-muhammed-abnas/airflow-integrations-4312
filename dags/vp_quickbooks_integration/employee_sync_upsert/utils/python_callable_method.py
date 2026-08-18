"""
Common utility methods for VP -> QBO Employee Sync.

Ports the Workato recipe
`014_503_psa_vantagepoint_employee_to_quickbooks` into Python callables
for the 3-DAG Airflow template (main -> dispatcher -> processor).
The processor maintains a VP <-> QBO employee mapping table backed by the
shared mapping_sync `map_employee` S3 collection (read/written here keyed by
VP Employee code — the SAME table employee_sync uses, but upsert dedups on
the VP Employee code rather than QBOID). See the collection helpers below.
"""
# pylint: disable=invalid-name,broad-exception-caught
import logging
from urllib.parse import quote
import rail
# The shared collection helpers + table/column constants come from common so
# the S3 access logic and SQLite identifiers can't drift across integrations.
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    collection_update,
    collection_upsert,
    unwrap_vp_response,
)
from vp_quickbooks_integration.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_COLUMNS,
    MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VP /employee poll filter + response extraction
# ---------------------------------------------------------------------------
def build_vp_employee_filter_method():
    """filterHash-style filter for the VP /employee polling GET.

    Two-sided `last_sync_time <= ModDate < current_sync_time` window so
    each poll claims a closed lower / open upper interval. The same
    `current_sync_time` is written back to the watermark Variable on
    success, making the next run's lower bound exactly this run's upper
    bound (no gap, no overlap).

    Matches the Postman-verified URL shape:
        ?filterHash[0][name]=ModDate&filterHash[0][value]=<last>
         &filterHash[0][type]=datetime&filterHash[0][opp]=>=
         &filterHash[0][condition]=AND&filterHash[0][seq]=0
         &filterHash[1][name]=ModDate&filterHash[1][value]=<current>
         &filterHash[1][type]=datetime&filterHash[1][opp]=<
         &filterHash[1][seq]=1

    Timestamp values and the `>=` / `<` operators are URL-encoded;
    brackets are left raw (Vantagepoint accepts PHP-style array keys).
    The operator's `filters` parameter is appended to the request URL
    as a raw query string starting with '?'.
    """
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


def extract_employee_list_method():
    """Normalize VP /employee response to a list of {Employee, ModDate, Name}.

    `Name` is carried alongside `Employee`/`ModDate` purely so the
    processor's error-capture step can render a labeled message
    ("Employee X (John Smith) - map sync failed: ...") even when
    get_single_employee_from_vp itself is what failed. It will be ''
    if the poll record has no FirstName/LastName.
    """
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
    """IfOperator test: did the VP poll return any rows?"""
    return len(rail.result('extract_employee_list') or []) > 0


# Collection access (read/write the shared map_employee collection) uses the
# shared helpers in common.python_callable_method — `collection_rows` /
# `collection_update`, imported above and called directly.


# ---------------------------------------------------------------------------
# Employee map (Workato `014-503 PSA Map Employee`) — read/written as the shared
# mapping_sync `map_employee` collection (NOT an Airflow Variable). Columns come
# from common.tables.MAP_EMPLOYEE_COLUMNS: Employee, QBOID, QBOVendorID,
# QBOVendorName, Name. This integration keys/dedups by the VP Employee code
# (Workato col1) — NOT QBOID (that is employee_sync's key into the same table).
# map_employee carries a UNIQUE index on Employee (MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS,
# one of the two independent indexes in MAP_EMPLOYEE_UNIQUE_INDEXES), so writes
# use a single atomic upsert keyed on Employee rather than DELETE-then-INSERT.
# ---------------------------------------------------------------------------


def _write_map_employee_row(employee, qbo_id, qbo_vendor_id, qbo_vendor_name,
                            name, context=None):
    """Upsert one map_employee row keyed by Employee via a single atomic
    S3UpsertCollectionOperator call.

    Keyed on MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS (Employee) — this integration's
    dedup key, NOT QBOID (that is employee_sync's key into the same table).
    ``INSERT ... ON CONFLICT(Employee) DO UPDATE`` replaces the existing row's
    non-key columns in one S3 commit (dup-free, idempotent, atomic), superseding
    the old DELETE-then-INSERT. NOTE: because map_employee also has a UNIQUE
    index on QBOID, this upsert raises if the proposed QBOID already belongs to a
    DIFFERENT Employee row — a genuine 1:1-mapping conflict that must surface
    rather than silently corrupt. Column ordering is driven by
    common.tables.MAP_EMPLOYEE_COLUMNS so SQLite identifiers can't drift.
    """
    context = context or rail.get_current_context()
    values = {
        'Employee': employee,
        'QBOID': qbo_id or '',
        'QBOVendorID': qbo_vendor_id or '',
        'QBOVendorName': qbo_vendor_name or '',
        'Name': name or '',
    }
    collection_upsert(
        MAP_EMPLOYEE_TABLE_NAME,
        MAP_EMPLOYEE_EMPLOYEE_UNIQUE_COLUMNS,
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


def get_employee_from_map_method():
    """Look up the shared map_employee collection by VP Employee code.

    Ports the Workato `search_entries` step (col1 = Employee). Returns the
    row dict (incl. _rowid) or None.
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
    return rows[0] if rows else None


def check_employee_exists_in_map_method():
    """IfOperator test: did get_employee_from_map return a row?"""
    return rail.result('get_employee_from_map') is not None


def update_employee_in_map_method():
    """Refresh an existing employee map row from VP.

    Mirrors the Workato `update_entry` step:
      col1 (Employee) <- VP.Employee
      col2 (QBOID)    <- VP.QBOID
      col5 (Name)     <- VP.FirstName + " " + VP.LastName
    `QBOVendorID` / `QBOVendorName` are preserved from the existing row.
    """
    record = _fetched_vp_employee()
    vp_employee = (
        (record.get('Employee') or '').strip()
        or _conf_employee_code()
    )
    if not vp_employee:
        logger.warning("Skipping map update — no VP Employee code available")
        return None

    # Reuse the row already fetched by the upstream get_employee_from_map task
    # (it carries _rowid for an in-place update); QBOVendorID/QBOVendorName are
    # preserved from that existing row.
    existing = rail.result('get_employee_from_map') or {}
    qbo_id = (record.get('QBOID') or '').strip()
    name = _full_name(record)

    rowid = existing.get('_rowid')
    if rowid is None:
        # No rowid (shouldn't happen on the update branch) — fall back to the
        # DELETE-then-INSERT upsert keyed by Employee.
        _write_map_employee_row(
            vp_employee, qbo_id,
            existing.get('QBOVendorID'), existing.get('QBOVendorName'), name,
        )
    else:
        # In-place update by rowid — single atomic statement. Employee and the
        # preserved QBOVendor* columns are left untouched.
        collection_update(
            MAP_EMPLOYEE_TABLE_NAME,
            f"UPDATE {MAP_EMPLOYEE_TABLE_NAME} "
            "SET QBOID = ?, Name = ? WHERE rowid = ?",
            [qbo_id, name, rowid],
        )
    logger.info(
        "Updated map_employee row: Employee=%s QBOID='%s' Name='%s'",
        vp_employee, qbo_id, name
    )
    return {
        'Employee': vp_employee,
        'QBOID': qbo_id,
        'QBOVendorID': existing.get('QBOVendorID') or '',
        'QBOVendorName': existing.get('QBOVendorName') or '',
        'Name': name,
    }


def add_employee_to_map_method():
    """Insert a new employee map row.

    Mirrors the Workato `add_entry` step:
      col1 (Employee) <- VP.Employee
      col5 (Name)     <- VP.FirstName + " " + VP.LastName
    `QBOID` / `QBOVendorID` / `QBOVendorName` are left as empty strings
    (Workato add_entry leaves them blank — they get populated later by
    other processes when QBO linkage is established).
    """
    record = _fetched_vp_employee()
    vp_employee = (
        (record.get('Employee') or '').strip()
        or _conf_employee_code()
    )
    if not vp_employee:
        logger.warning("Skipping map add — no VP Employee code available")
        return None

    name = _full_name(record)
    # QBOID / QBOVendorID / QBOVendorName left blank (Workato add_entry leaves
    # them blank — populated later when QBO linkage is established).
    _write_map_employee_row(vp_employee, '', '', '', name)
    logger.info(
        "Added map_employee row: Employee=%s Name='%s'", vp_employee, name
    )
    return {
        'Employee': vp_employee,
        'QBOID': '',
        'QBOVendorID': '',
        'QBOVendorName': '',
        'Name': name,
    }


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps the processor DAG SUCCESS
# so the dispatcher's WaitForDagRunsSensor never sees a failed run and
# GatherResultsFromDagRunsOperator can collect the error dict.)
# ---------------------------------------------------------------------------
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


