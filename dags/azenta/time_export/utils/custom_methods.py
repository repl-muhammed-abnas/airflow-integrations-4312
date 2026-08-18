"""
Helper methods for Azenta Oracle PPM Time Export Integration (FI017)
"""
import json
import logging
from datetime import datetime, timedelta, timezone as dt_timezone
from xml.sax.saxutils import escape as xml_escape
import pendulum
import rail
import rail.lib.readers
from lxml import etree

TWB_DATE_FORMAT = "%Y%m%d"
ORACLE_DATE_FORMAT = "%Y-%m-%d"

# Namespaces confirmed via the client-validated Postman collection
# ("Oracle PPM SOAP - ProjectTimecardService (Unprocessed Labor Transaction V3) Copy 3")
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
WSSE_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
WSU_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
TYP_NS = "http://xmlns.oracle.com/apps/projects/costing/transactions/transactionServiceV3/types/"
TXN_NS = "http://xmlns.oracle.com/apps/projects/costing/transactions/transactionServiceV3/"

# Stands in for the WS-Security header inside the envelope string returned by the
# build_*_soap_envelope PythonOperators below. That return value is persisted to XCom in
# cleartext, so it must never carry the real Oracle password — main_dag.py's SimpleHttpOperator
# substitutes the real header in via Jinja at render time (conn.get(...) | wsse_header), which is
# never persisted to XCom.
WSSE_HEADER_PLACEHOLDER = "__WSSE_SECURITY_HEADER_PLACEHOLDER__"


def get_logging_details(timezone, export_file_prefix, accounting_cutoff_hour=17):
    today = pendulum.now(timezone)
    current_time = today.strftime('%Y%m%d_%H%M%S')

    # Grace period: on day 1 of month M+1 before the cutoff hour, also cover previous month.
    cutoff_timestamp = today.start_of('month').replace(
        hour=accounting_cutoff_hour, minute=0, second=0
    )
    if today.day == 1 and today <= cutoff_timestamp:
        start_dt = today.subtract(months=1).start_of('month')
    else:
        start_dt = today.start_of('month')

    return {
        "current_time": current_time,
        "time_export_filename": f"{export_file_prefix}_{current_time}",
        "time_export_filename_nodata": f"{export_file_prefix}_{current_time}_N",
        "time_export_filename_cancelled": f"{export_file_prefix}_{current_time}_C",
        "timezone": timezone,
        "export_start_date_json": {"year": start_dt.year, "month": start_dt.month, "day": start_dt.day},
        "export_end_date_json": {"year": today.year, "month": today.month, "day": today.day}
    }


def format_entry_date(date_str):
    """Convert Replicon date (YYYYMMDD) to Oracle-required format (YYYY-MM-DD)."""
    if not date_str:
        return ''
    date_str = str(date_str).strip()
    if len(date_str) == 8 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, TWB_DATE_FORMAT).strftime(ORACLE_DATE_FORMAT)
        except ValueError:
            return date_str
    return date_str


def filter_by_eligibility(records, eligible_project_statuses, timezone, accounting_cutoff_hour):
    """
    Apply two post-load filters before Oracle posting:

    1. Project status: skip records where Project_Status is set but not in eligible_project_statuses.
       Pass-through if the Project_Status column is absent from the export.
    2. Accounting cutoff: entries from month M are eligible if approved on or before day 1
       of month M+1 at accounting_cutoff_hour (Eastern).
       Pass-through if Approval_Date column is absent from the export.

    `records` may be a collection reference (as returned by CreateCollectionOperator) rather
    than a materialized list — resolved here via get_data_reader, matching the pattern in
    dags/dxctechnology/c1_wbs_import_v9/utils/python_callable_method.py.
    """
    if not records:
        return records
    if isinstance(records, str):
        with rail.lib.readers.get_data_reader(records) as reader:
            records = list(reader)

    now_tz = pendulum.now(timezone)
    eligible = []
    dropped_project_status = []
    dropped_approval_window = []
    for item in records:
        ref = item.get('Time_Entry_ID')
        project_status = item.get('Project_Status')
        if project_status and project_status not in eligible_project_statuses:
            dropped_project_status.append(ref)
            continue
        if not _is_within_approval_window(item, now_tz, accounting_cutoff_hour):
            dropped_approval_window.append(ref)
            continue
        eligible.append(item)

    if dropped_project_status:
        logging.warning(
            "filter_by_eligibility dropped %d record(s) for ineligible Project_Status: %s",
            len(dropped_project_status), dropped_project_status
        )
    if dropped_approval_window:
        logging.warning(
            "filter_by_eligibility dropped %d record(s) outside the accounting cutoff window: %s",
            len(dropped_approval_window), dropped_approval_window
        )

    return eligible


def _is_within_approval_window(item, now_tz, cutoff_hour):
    """
    Return True if the entry's approval date falls within the allowed accounting window.
    Cutoff for entries from month M is: day 1 of month M+1 at cutoff_hour (in now_tz timezone).
    Returns True (pass-through) when Approval_Date or Entry_Date is absent or unparseable.
    """
    entry_date_str = str(item.get('Entry_Date', '') or '').strip()
    approval_date_str = str(item.get('Approval_Date', '') or '').strip()

    if not approval_date_str or not entry_date_str:
        return True

    if len(entry_date_str) != 8 or not entry_date_str.isdigit():
        return True

    try:
        entry_dt = pendulum.from_format(entry_date_str, 'YYYYMMDD', tz=now_tz.timezone_name)
    except ValueError:
        return True

    first_of_next_month = entry_dt.end_of('month').add(days=1)
    cutoff_dt = first_of_next_month.replace(hour=cutoff_hour, minute=0, second=0)

    try:
        approval_clean = approval_date_str.replace('/', '-')
        if len(approval_clean) == 8 and approval_clean.isdigit():
            approval_dt = pendulum.from_format(approval_clean, 'YYYYMMDD', tz=now_tz.timezone_name)
        else:
            approval_dt = pendulum.parse(approval_clean, strict=False, tz=now_tz.timezone_name)
        return approval_dt <= cutoff_dt
    except (ValueError, TypeError) as exc:
        logging.warning(
            "Unparseable Approval_Date %r for entry %r — passing record through: %s",
            approval_date_str, item.get('Time_Entry_ID'), exc
        )
        return True


def get_distinct_primary_functions(login_to_function_map):
    """Distinct, order-stable Primary Function names across the resolved batch — used to build the
    one publicWorkers generic-resource lookup call, instead of a per-entry Oracle round-trip."""
    seen = []
    for primary_function in (login_to_function_map or {}).values():
        if primary_function and primary_function not in seen:
            seen.append(primary_function)
    return seen


def build_generic_resource_query(primary_functions):
    """
    GET query params for the Oracle HCM publicWorkers lookup that resolves each distinct Primary
    Function name to its generic-resource PersonNumber/PersonName.

    Validated against the live trial tenant: a bare Function name (e.g. "Refrigeration
    Engineering") matches several site-qualified generic resources ("...BLG"/"...CHE"/"...MAN"),
    so this relies on Replicon's Primary Function role name already being the fully
    site-qualified string that matches Oracle's DisplayName exactly (client-corrected in
    Replicon) — hence exact DisplayName= equality per name, OR'd together in one call.
    Returns None when there is nothing to resolve (empty batch).

    BusinessUnitName/ExpenditureOrganizationName are not resolved from Oracle here — they come
    directly from the "Business Unit" column (Replicon user profile) in the "Time Data Export -
    Oracle" report, so this lookup only needs PersonNumber/PersonId/DisplayName.
    """
    if not primary_functions:
        return None
    escaped = [name.replace("'", "''") for name in primary_functions]
    return {
        "q": " or ".join(f"DisplayName='{name}'" for name in escaped),
        "fields": "PersonNumber,DisplayName,PersonId",
        "onlyData": "true",
        "limit": "500"
    }


def build_person_number_map(publicworkers_response):
    """Turn the publicWorkers lookup response into DisplayName → {PersonNumber, PersonName}.
    SimpleHttpOperator hands back the raw JSON response body as a string — must be parsed before use.
    build_generic_resource_query caps this lookup at limit=500 distinct Primary Function names; if
    Oracle's response reports hasMore=true, that cap was hit and the map below is truncated — logged
    as a warning rather than silently dropping the excess names into the SKIPPED report bucket."""
    if isinstance(publicworkers_response, str):
        publicworkers_response = json.loads(publicworkers_response) if publicworkers_response else {}
    publicworkers_response = publicworkers_response or {}
    if publicworkers_response.get('hasMore'):
        logging.warning(
            "build_person_number_map: publicWorkers response has hasMore=true — the limit=500 "
            "query truncated results; some Primary Functions will have no resolved person and "
            "their records will be reported as SKIPPED"
        )
    items = publicworkers_response.get('items', [])
    result = {}
    for entry in items:
        display_name = entry.get('DisplayName')
        person_number = entry.get('PersonNumber')
        if display_name and person_number is not None:
            result[display_name] = {
                'PersonNumber': str(person_number),
                'PersonName': display_name,
            }
    return result


def get_distinct_logins(records):
    """Distinct, order-stable Login_Name values across the eligible records — used to build the
    BulkGetUsers3 request once for the whole batch, instead of once per loop iteration."""
    seen = []
    for item in records:
        login = item.get('Login_Name')
        if login and login not in seen:
            seen.append(login)
    return seen


def build_login_to_user_uri_map(logins, bulk_get_users_response):
    """
    Pair the BulkGetUsers3 request's login list with its response. Each request row sets
    parameterCorrelationId to the login (see build_bulk_get_users_request), so correlate by that
    field when the response echoes it back. If no entry carries parameterCorrelationId at all
    (unconfirmed whether this API echoes it), fall back to positional zip — the API preserves
    request order and, per dataLoadOptionUri=omit-data-if-insufficient-access-permission, still
    returns one response entry per requested user rather than dropping it, so positional zip stays
    aligned in that case too.
    """
    response = bulk_get_users_response or []
    result = {}
    if any(entry and entry.get('parameterCorrelationId') for entry in response):
        for entry in response:
            if not entry or entry.get('error'):
                continue
            login = entry.get('parameterCorrelationId')
            user_details = entry.get('userDetails') or {}
            uri = user_details.get('uri')
            if login and uri:
                result[login] = uri
        return result
    for login, entry in zip(logins, response):
        if not entry or entry.get('error'):
            continue
        user_details = entry.get('userDetails') or {}
        uri = user_details.get('uri')
        if uri:
            result[login] = uri
    return result


def extract_primary_function(schedule_entries):
    """From one user's BulkGetProjectRoleAssignmentScheduleForUsers `schedule` list, return the
    displayText of the projectRole flagged isPrimary=true, or None if not found."""
    for schedule_entry in schedule_entries or []:
        for project_role in schedule_entry.get('projectRoles') or []:
            if project_role.get('isPrimary'):
                return (project_role.get('projectRole') or {}).get('displayText')
    return None


def build_login_to_function_map(login_to_user_uri, roles_response):
    """
    Combine the login→userUri map with BulkGetProjectRoleAssignmentScheduleForUsers's response
    (correlated via each entry's own `userUri` field, not position) into login→PrimaryFunction.
    A user with a service error, no schedule, or no isPrimary=true role is simply absent from the
    result — build_oracle_rows' skip path handles that (see its docstring).
    """
    uri_to_login = {uri: login for login, uri in login_to_user_uri.items()}
    result = {}
    for entry in roles_response or []:
        if not entry or entry.get('error'):
            continue
        login = uri_to_login.get(entry.get('userUri'))
        if not login:
            continue
        primary_function = extract_primary_function(entry.get('schedule'))
        if primary_function:
            result[login] = primary_function
    return result


def build_receive_timecard_row(item, person, batch_name, config):
    """
    Build one Oracle receiveTimecardTransaction row dict from a single Replicon time entry item,
    matching the field set/names validated in the Postman collection ("Oracle PPM SOAP -
    ProjectTimecardService (Unprocessed Labor Transaction V3) Copy 3").

    Column name assumptions (based on 'Time Data Export - Oracle' format):
      Time_Entry_ID        — Replicon time entry identifier (idempotency key)
      User                 — Real employee display name; mapped to ExpenditureComment
      Project_Code         — Oracle ProjectNumber
      Project_Name         — Oracle ProjectName; sent alongside ProjectNumber so Oracle can still
                             resolve the project if Project_Code doesn't match a valid project ID
      Task_Code            — Oracle TaskNumber
      Task_Name            — Oracle TaskName; sent alongside TaskNumber — Oracle only requires one
                             of TaskId/TaskName/TaskNumber, so this covers rows with a blank Task_Code
      Entry_Date           — Expenditure item date (YYYYMMDD from Replicon)
      Hours                — Quantity in hours
      Business_Unit        — Replicon user-profile Business Unit; used verbatim for BOTH
                             BusinessUnitName (header) and ExpenditureOrganizationName (line)

    Negative Hours (Replicon hours-reduction corrections, resent under the same Time_Entry_ID) get
    UnmatchedNegativeTxnFlag=config.oracle_soap_unmatched_negative_txn_flag — see the flag's own
    comment below for why this is always 'Y', not conditional on anything about the record.

    `person` is the generic-resource {PersonNumber, PersonName} dict resolved from
    person_number_map by the caller (build_oracle_rows) — NOT the real employee.

    "OrigTransactionReference" (this row's request field, below) and "OriginalTransactionReference"
    (the response's echoed correlation field parsed in _parse_soap_result_rows) are deliberately
    different element names — that's how Oracle's WSDL names them, confirmed against a real trial
    response. Correlation between request and response works because both carry the *same value*
    (this prefix+Time_Entry_ID string), not because the element names match.
    """
    business_unit = item.get('Business_Unit', '')
    hours_raw = item.get('Hours', 0)
    try:
        quantity = float(hours_raw) if str(hours_raw).strip() else 0.0
    except (ValueError, TypeError):
        logging.warning(
            "build_receive_timecard_row: unparseable Hours %r for Time_Entry_ID %r — using 0.0",
            hours_raw, item.get('Time_Entry_ID', '')
        )
        quantity = 0.0
    row = {
        "TransactionType": "LABOR",
        "BusinessUnitName": business_unit,
        "SourceName": config.oracle_soap_source_name,
        "DocumentName": config.oracle_soap_document_name,
        "DocumentEntryName": config.oracle_soap_document_entry_name,
        "BatchName": batch_name,
        "ExpenditureItemDate": format_entry_date(item.get('Entry_Date', '')),
        "PersonNumber": person['PersonNumber'],
        "PersonName": person['PersonName'],
        "ProjectNumber": str(item.get('Project_Code', '')),
        "TaskNumber": str(item.get('Task_Code', '')),
        "ExpenditureTypeName": config.oracle_soap_expenditure_type_name,
        "ExpenditureOrganizationName": business_unit,
        "Quantity": quantity,
        "UnitOfMeasure": config.oracle_soap_unit_of_measure,
        "OrigTransactionReference": f"{config.oracle_transaction_ref_prefix}{item.get('Time_Entry_ID', '')}",
        "ExpenditureComment": item.get('User', ''),
    }

    # Oracle requires this on any negative-Quantity row. Always 'Y' (unmatched) — Replicon's
    # hours-reduction corrections reuse the same Time_Entry_ID as the original entry, but that's
    # Replicon-side tracking only; Oracle's own negative-transaction matching keys off employee/
    # org/date/expenditure-type/project/task (not OrigTransactionReference) and additionally
    # requires the original to have already been processed by Oracle's separate Import Costs ESS
    # job (out of scope for this integration, run independently) — 'N' would intermittently fail
    # depending on that job's timing. 'Y' is Oracle's documented path for exactly this case:
    # summary-level negative adjustments that aren't required to match a prior transaction.
    if quantity < 0:
        row["UnmatchedNegativeTxnFlag"] = config.oracle_soap_unmatched_negative_txn_flag

    project_name = str(item.get('Project_Name', '') or '').strip()
    if project_name:
        row["ProjectName"] = project_name

    task_name = str(item.get('Task_Name', '') or '').strip()
    if task_name:
        row["TaskName"] = task_name

    return row


def build_oracle_rows(records, login_to_function_map, person_number_map, batch_name, config):
    """
    Resolve each eligible record's Primary Function to its Oracle generic-resource person and
    build its receiveTimecardTransaction row, in a single in-process pass over `records`.

    A record is skipped (not posted) when its login has no resolved Primary Function, or that
    Primary Function has no matching Oracle generic resource. Skipped records are reported back
    by reference — instead of silently dropping them from the export — so callers can surface the
    count/refs to the client and in logs.
    """
    rows = []
    skipped = []
    for item in records or []:
        login = item.get('Login_Name')
        primary_function = login_to_function_map.get(login) if login else None
        person = person_number_map.get(primary_function) if primary_function else None
        if not person:
            skipped.append(str(item.get('Time_Entry_ID', '') or login or 'unknown'))
            continue
        rows.append(build_receive_timecard_row(item, person, batch_name, config))
    return {'rows': rows, 'skipped': skipped}


def format_no_rows_message(build_oracle_rows_result):
    """Human-readable summary for when build_oracle_rows resolved zero postable rows."""
    skipped = (build_oracle_rows_result or {}).get('skipped') or []
    if not skipped:
        return "No rows were built for Oracle posting, and no records were skipped (unexpected)."
    return (
        f"No rows were resolved for Oracle posting — all {len(skipped)} eligible record(s) were "
        f"skipped because their Primary Function had no matching Oracle generic-resource person. "
        f"Skipped references: {', '.join(skipped)}"
    )


def compute_batch_name(records, timezone=None):
    """One shared BatchName for the whole bulk submission — 'Replicon - Timesheets - {YYYYMMDD}',
    where the date is the Monday (start of week, per tech spec) of the earliest Entry_Date across
    the eligible records for this run. Computed once in build_oracle_rows's single in-process pass
    so every row shares the same value. Falls back to today's start-of-week — in `timezone` if given,
    else UTC — if no record has a parseable Entry_Date (shouldn't happen — has_eligible_records
    already guarantees records)."""
    entry_dates = []
    for record in records or []:
        date_str = str(record.get('Entry_Date', '')).strip()
        if len(date_str) == 8 and date_str.isdigit():
            try:
                entry_dates.append(datetime.strptime(date_str, TWB_DATE_FORMAT))
            except ValueError:
                continue
    if entry_dates:
        earliest = min(entry_dates)
    elif timezone:
        earliest = pendulum.now(timezone)
    else:
        earliest = datetime.now(dt_timezone.utc)
    start_of_week = earliest - timedelta(days=earliest.weekday())
    return f"Replicon - Timesheets - {start_of_week.strftime('%Y%m%d')}"


def build_wsse_security_header(username, password):
    """
    Build the WS-Security UsernameToken + wsu:Timestamp header fragment, mirroring the Postman
    collection's pre-request script (10-minute Created/Expires window, UTC, ISO-8601).
    """
    now = datetime.now(dt_timezone.utc)
    created = now.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
    expires = (now + timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
    return (
        f'<wsse:Security xmlns:wsse="{WSSE_NS}">'
        f'<wsu:Timestamp xmlns:wsu="{WSU_NS}">'
        f'<wsu:Created>{created}</wsu:Created>'
        f'<wsu:Expires>{expires}</wsu:Expires>'
        f'</wsu:Timestamp>'
        f'<wsse:UsernameToken>'
        f'<wsse:Username>{xml_escape(username)}</wsse:Username>'
        f'<wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">'
        f'{xml_escape(password)}</wsse:Password>'
        f'</wsse:UsernameToken>'
        f'</wsse:Security>'
    )


def wsse_security_header_for_connection(connection):
    """
    Jinja filter (registered as `wsse_header` in main_dag.py's create_airflow_dag) — builds the
    real WS-Security header from a full Airflow Connection object, obtained via the built-in `conn`
    Jinja accessor at task-render time. Keeps the real password out of build_bulk_soap_envelope's/
    build_validate_soap_envelope's return value, which Airflow persists to XCom in cleartext.
    """
    return build_wsse_security_header(connection.login, connection.password)


def _build_timecard_envelope(rows, operation, extra_flags=""):
    """
    Shared SOAP envelope builder for both receiveTimecardTransaction and validateTimecardTransaction
    — same row schema, differing only in the wrapped operation element and (for receive only) the
    partialFailureAllowed/fullConfirmation flags. Embeds WSSE_HEADER_PLACEHOLDER in place of the
    WS-Security header — see WSSE_HEADER_PLACEHOLDER's docstring for why.
    """
    row_fragments = "".join(_build_timecard_row_fragment(row) for row in rows)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<soapenv:Envelope xmlns:soapenv="{SOAP_NS}" xmlns:typ="{TYP_NS}" xmlns:txn="{TXN_NS}">'
        f'<soapenv:Header>{WSSE_HEADER_PLACEHOLDER}</soapenv:Header>'
        f'<soapenv:Body><typ:{operation}>{row_fragments}{extra_flags}</typ:{operation}></soapenv:Body>'
        f'</soapenv:Envelope>'
    )


def build_bulk_soap_envelope(rows):
    """
    Build the receiveTimecardTransaction SOAP envelope wrapping one <typ:list> block per row, for
    a single bulk submission to Oracle's ProjectTimecardService — per the validated Postman
    collection. WS-Security credentials are injected later, at task-render time (see
    WSSE_HEADER_PLACEHOLDER), not sourced from a connection here.
    """
    return _build_timecard_envelope(
        rows, 'receiveTimecardTransaction',
        extra_flags=(
            '<typ:partialFailureAllowed>true</typ:partialFailureAllowed>'
            '<typ:fullConfirmation>true</typ:fullConfirmation>'
        )
    )


def build_validate_soap_envelope(rows):
    """
    Build the validateTimecardTransaction SOAP envelope — a dry-run check against the same rows,
    called before the actual receiveTimecardTransaction post, per the client-provided request
    template. No partialFailureAllowed/fullConfirmation flags (not part of that template).
    """
    return _build_timecard_envelope(rows, 'validateTimecardTransaction')


def _build_timecard_row_fragment(row):
    fields = "".join(
        f'<txn:{key}>{xml_escape(str(value))}</txn:{key}>'
        for key, value in row.items()
    )
    return f'<typ:list>{fields}</typ:list>'


# receiveTimecardTransaction-only Status values that mean "accepted by Oracle" even though they
# aren't 'SUCCESS' — confirmed against a real trial response: Oracle can return
# PJC_TXN_XFACE_IS_PENDING ("The transaction is accepted and is pending to be processed.") when the
# row is accepted but still queued for its own async interface/cost-processing job. Does not apply
# to validateTimecardTransaction, which has no such interface-pending concept.
RECEIVE_NON_FAILURE_STATUSES = {'SUCCESS', 'PJC_TXN_XFACE_IS_PENDING'}


def has_soap_fault(response_text):
    """
    True if response_text contains a SOAP Fault element, or any returned row has a Status outside
    RECEIVE_NON_FAILURE_STATUSES. receiveTimecardTransaction is submitted with
    partialFailureAllowed=true/fullConfirmation=true, so Oracle can reject individual rows under
    HTTP 200 with no envelope Fault — the per-row check mirrors has_validation_failure. Fail-safe:
    an unparsable response, or one with no result rows at all, is treated as a fault rather than
    silently passing as success.
    """
    if not response_text:
        return True
    try:
        root = etree.fromstring(response_text.encode('utf-8'))
    except etree.XMLSyntaxError:
        return True
    if root.xpath('//*[local-name()="Fault"]'):
        return True
    results = root.xpath('//*[local-name()="result"]')
    if not results:
        return True
    return any(
        (result.xpath('./*[local-name()="Status"]/text()') or [''])[0].strip() not in RECEIVE_NON_FAILURE_STATUSES
        for result in results
    )


def format_soap_fault_message(response_text):
    """
    Human-readable failure message for a receiveTimecardTransaction response: an envelope Fault's
    faultstring/detail, or — since has_soap_fault also flags per-row Status outside
    RECEIVE_NON_FAILURE_STATUSES under partialFailureAllowed/fullConfirmation — a
    one-line-per-failing-row summary mirroring format_validation_failure_message.
    """
    if not response_text:
        return "SOAP fault: empty response"
    try:
        root = etree.fromstring(response_text.encode('utf-8'))
    except etree.XMLSyntaxError:
        return f"SOAP fault: response was not well-formed XML: {response_text[:500]}"

    fault_strings = root.xpath('//*[local-name()="faultstring"]/text()')
    if fault_strings:
        detail_texts = root.xpath('//*[local-name()="detail"]//text()')
        message = fault_strings[0]
        if detail_texts:
            message += " | detail: " + " ".join(t.strip() for t in detail_texts if t.strip())
        return message

    lines = []
    for result in root.xpath('//*[local-name()="result"]'):
        status = (result.xpath('./*[local-name()="Status"]/text()') or ['UNKNOWN'])[0].strip()
        if status in RECEIVE_NON_FAILURE_STATUSES:
            continue
        ref = (result.xpath('./*[local-name()="OriginalTransactionReference"]/text()')
               or ['unknown reference'])[0].strip()
        messages = [m.strip() for m in result.xpath('.//*[local-name()="MessageText"]/text()') if m.strip()]
        if not messages:
            status_message = (result.xpath('./*[local-name()="StatusMessage"]/text()') or [status])[0].strip()
            messages = [status_message]
        lines.append(f"{ref}: {'; '.join(messages)}")

    if lines:
        return "SOAP fault: posting failed for the following entries — " + " | ".join(lines)
    return "SOAP fault: no faultstring present in response"


def has_validation_failure(response_text):
    """
    True if any row in a validateTimecardTransactionResponse has Status != 'SUCCESS' (the only
    confirmed success literal — see the client-provided sample response), the response has an
    envelope Fault, or the response is empty/unparsable. Fail-safe like has_soap_fault.
    """
    if not response_text:
        return True
    try:
        root = etree.fromstring(response_text.encode('utf-8'))
    except etree.XMLSyntaxError:
        return True
    if root.xpath('//*[local-name()="Fault"]'):
        return True
    results = root.xpath('//*[local-name()="result"]')
    if not results:
        return True
    return any(
        (result.xpath('./*[local-name()="Status"]/text()') or [''])[0].strip() != 'SUCCESS'
        for result in results
    )


def _parse_soap_result_rows(response_text):
    """
    Parse every <result> row's OriginalTransactionReference/Status/message from a
    validateTimecardTransaction or receiveTimecardTransaction SOAP response — unlike
    has_soap_fault/format_soap_fault_message/format_validation_failure_message (which only surface
    failing rows), this returns one entry per row regardless of status, for the per-record CSV
    report. Returns {} for an empty/unparsable response or an envelope Fault — same fail-safe
    posture as the other parsers, since there's nothing row-level to report in either case.
    """
    if not response_text:
        return {}
    try:
        root = etree.fromstring(response_text.encode('utf-8'))
    except etree.XMLSyntaxError:
        return {}
    if root.xpath('//*[local-name()="Fault"]'):
        return {}
    result_map = {}
    for result in root.xpath('//*[local-name()="result"]'):
        ref = (result.xpath('./*[local-name()="OriginalTransactionReference"]/text()') or [''])[0].strip()
        if not ref:
            continue
        status = (result.xpath('./*[local-name()="Status"]/text()') or ['UNKNOWN'])[0].strip()
        messages = [m.strip() for m in result.xpath('.//*[local-name()="MessageText"]/text()') if m.strip()]
        if not messages and status != 'SUCCESS':
            status_message = (result.xpath('./*[local-name()="StatusMessage"]/text()') or [''])[0].strip()
            if status_message:
                messages = [status_message]
        result_map[ref] = {'status': status, 'message': '; '.join(messages)}
    return result_map


# Raw Oracle/internal status literals that report as report-level 'Success' — everything else
# falls to 'Error', except the internal-only literals below that fall to 'Exception' (the record
# never reached Oracle at all, or Oracle's response didn't confirm/deny it).
_REPORT_SUCCESS_STATUSES = RECEIVE_NON_FAILURE_STATUSES
_REPORT_EXCEPTION_STATUSES = {'SKIPPED', 'UNKNOWN'}


def _categorize_report_status(raw_status):
    """
    Collapse a raw Oracle Status (or the internal 'SKIPPED'/'UNKNOWN' literals) down to the three
    report-level categories the client asked for: Success (Oracle accepted the row, including the
    PJC_TXN_XFACE_IS_PENDING accepted-but-queued case), Exception (the record never reached Oracle,
    or its outcome couldn't be determined from the response), Error (Oracle explicitly rejected it).
    """
    if raw_status in _REPORT_SUCCESS_STATUSES:
        return 'Success'
    if raw_status in _REPORT_EXCEPTION_STATUSES:
        return 'Exception'
    return 'Error'


def filter_error_rows(rows):
    """Keep only rows the client needs to act on (Error/Exception) — used for the
    validation-failure report, which shouldn't restate rows that already passed validation."""
    return [row for row in (rows or []) if row.get('Status') != 'Success']


def build_export_report_rows(eligible_records, oracle_rows_result, response_text, config):
    """
    Per-record CSV report rows for the success/validation-failure/posting-failure emails: one row
    per Oracle timecard row that was submitted (Login/Entry Date/Hours/Project Id from the matching
    eligible record, Status/Message from response_text's per-row result — see
    _parse_soap_result_rows), plus one row per build_oracle_rows-skipped record (Status='SKIPPED' —
    these never reached Oracle at all, so they'd otherwise be invisible in the report). The raw
    Oracle Status is collapsed to Success/Exception/Error via _categorize_report_status.
    """
    ref_prefix = config.oracle_transaction_ref_prefix
    record_by_ref = {
        f"{ref_prefix}{item.get('Time_Entry_ID', '')}": item for item in eligible_records or []
    }
    record_by_time_entry_id = {
        str(item.get('Time_Entry_ID', '')): item for item in eligible_records or []
    }
    status_map = _parse_soap_result_rows(response_text)

    rows = []
    for row in (oracle_rows_result or {}).get('rows', []):
        ref = row.get('OrigTransactionReference', '')
        record = record_by_ref.get(ref, {})
        status_info = status_map.get(ref, {})
        raw_status = status_info.get('status', 'UNKNOWN')
        rows.append({
            'Login': record.get('Login_Name', ''),
            'Project Id': record.get('Project_Code', ''),
            'Entry Date': format_entry_date(record.get('Entry_Date', '')),
            'Hours': record.get('Hours', ''),
            'Status': _categorize_report_status(raw_status),
            'Message': status_info.get('message', ''),
        })

    for skipped_ref in (oracle_rows_result or {}).get('skipped', []):
        record = record_by_time_entry_id.get(str(skipped_ref))
        rows.append({
            'Login': record.get('Login_Name', '') if record else str(skipped_ref),
            'Project Id': record.get('Project_Code', '') if record else '',
            'Entry Date': format_entry_date(record.get('Entry_Date', '')) if record else '',
            'Hours': record.get('Hours', '') if record else '',
            'Status': _categorize_report_status('SKIPPED'),
            'Message': "No matching Oracle generic-resource person found for this record's Primary Function",
        })

    return rows


def format_validation_failure_message(response_text):
    """Human-readable, one-line-per-failing-row summary of a validateTimecardTransaction failure."""
    if not response_text:
        return "Validation failed: empty response"
    try:
        root = etree.fromstring(response_text.encode('utf-8'))
    except etree.XMLSyntaxError:
        return f"Validation failed: response was not well-formed XML: {response_text[:500]}"
    if root.xpath('//*[local-name()="Fault"]'):
        return format_soap_fault_message(response_text)

    lines = []
    for result in root.xpath('//*[local-name()="result"]'):
        status = (result.xpath('./*[local-name()="Status"]/text()') or ['UNKNOWN'])[0].strip()
        if status == 'SUCCESS':
            continue
        ref = (result.xpath('./*[local-name()="OriginalTransactionReference"]/text()')
               or ['unknown reference'])[0].strip()
        messages = [m.strip() for m in result.xpath('.//*[local-name()="MessageText"]/text()') if m.strip()]
        if not messages:
            status_message = (result.xpath('./*[local-name()="StatusMessage"]/text()') or [status])[0].strip()
            messages = [status_message]
        lines.append(f"{ref}: {'; '.join(messages)}")

    if not lines:
        return "Validation failed: no failing rows found in response (unexpected)"
    return "Validation failed for the following entries — " + " | ".join(lines)


