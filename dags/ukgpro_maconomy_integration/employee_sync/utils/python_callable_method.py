"""
Common utility methods for UKG Pro → Maconomy Employee Sync integration.
"""
import logging
import rail

log = logging.getLogger(__name__)


MAX_REHIRE_REVISIONS = 9

MN_HEADERS_V6 = {
    'Accept': (
        'application/vnd.deltek.maconomy.containers+json; version=6.0'
    ),
    'Content-Type': (
        'application/vnd.deltek.maconomy.containers+json; version=6.0'
    ),
}


def mn_quote(value):
    """Quote a value for a Maconomy restriction expression, escaping embedded single quotes."""
    return "'" + str(value or '').replace("'", "''") + "'"



def resolve_maconomy_company_number():
    """Return the resolved Maconomy company number from the company search XCom.
    Falls back to companyCode from employment-details when the company was just created."""
    company_records = _get_mn_records('get_company_info_from_maconomy')
    if company_records:
        return company_records[0].get('data', {}).get('companynumber')
    return (rail.result('get_employment_details_from_ukgpro') or {}).get('companyCode')


def build_company_restriction(company_code, company_name):
    """Build Maconomy companyinfo filter restriction.
    Prefers companynumber (stable) over name1 (mutable display name)."""
    if company_code:
        return 'companynumber=' + mn_quote(company_code)
    return 'name1=' + mn_quote(company_name)


def build_employee_loop_restriction(employee_number, company_number, max_revisions=MAX_REHIRE_REVISIONS):
    """Build OR restriction covering base + all _r1.._r{max} rehire variants in one search."""
    parts = ['employeenumber=' + mn_quote(employee_number)]
    for i in range(1, max_revisions + 1):
        parts.append('employeenumber=' + mn_quote(f'{employee_number}_r{i}'))
    restriction = ' or '.join(parts)
    if company_number:
        restriction = (
            '(' + restriction + ') and companynumber=' + mn_quote(company_number)
        )
    return restriction


def _get_mn_records(task_id, pane='filter'):
    """Extract records list from a MaconomyCustomActionOperator SEARCH XCom."""
    result = rail.result(task_id)
    if not result or 'data' not in result:
        return []
    try:
        return result['data']['panes'][pane]['records']
    except (KeyError, TypeError):
        return []


def _find_active_mn_employee_number(records):
    """Return the employeenumber of the single non-blocked, no-dateendemployment record.
    Raises RuntimeError if multiple active records exist (data integrity violation)."""
    active = [
        r for r in (records or [])
        if not r.get('data', {}).get('blocked', False)
        and not r.get('data', {}).get('dateendemployment', '')
    ]
    if len(active) > 1:
        numbers = [r.get('data', {}).get('employeenumber') for r in active]
        raise RuntimeError(
            f"Multiple active Maconomy employee records found: {numbers} — "
            "cannot determine which to update"
        )
    if active:
        return active[0].get('data', {}).get('employeenumber')
    return None


# ── Router routing checks ──────────────────────────────────────────────────

def check_status_for_update():
    """UKG=Active AND exactly one non-blocked MN record found in the loop search."""
    conf = rail.get_current_context()['dag_run'].conf
    if conf.get('employeeStatusCode') != 'A':
        return False
    records = _get_mn_records('search_employee_in_maconomy')
    if not records:
        return False
    return _find_active_mn_employee_number(records) is not None


def check_status_for_rehire():
    """UKG=Active AND records exist in MN but ALL are blocked — create new suffixed record."""
    conf = rail.get_current_context()['dag_run'].conf
    if conf.get('employeeStatusCode') != 'A':
        return False
    records = _get_mn_records('search_employee_in_maconomy')
    if not records:
        return False
    # Records found but none active → all terminated → rehire
    return _find_active_mn_employee_number(records) is None


def check_status_for_termination():
    """UKG=Terminated with date AND an active MN record found in the loop search."""
    conf = rail.get_current_context()['dag_run'].conf
    if conf.get('employeeStatusCode') != 'T':
        return False
    if not conf.get('dateOfTermination'):
        return False
    records = _get_mn_records('search_employee_in_maconomy')
    if not records:
        return False
    return _find_active_mn_employee_number(records) is not None


def check_employee_active_in_ukgpro():
    """Check if employee is Active in UKG Pro (new employee / post-transfer-no-match path)."""
    conf = rail.get_current_context()['dag_run'].conf
    return conf.get('employeeStatusCode') == 'A'


def check_transfer_detected():
    """True when the remark1 search found a record under a different company or base
    employee number. Signals a transfer: UKG re-issued this person's record."""
    conf = rail.get_current_context()['dag_run'].conf
    if conf.get('employeeStatusCode') != 'A':
        return False
    current_number = conf.get('employeeNumber', '')
    current_company = conf.get('maconomy_company_number')
    records = _get_mn_records('search_by_employeeid_in_maconomy')
    for record in records:
        data = record.get('data', {})
        head, sep, tail = str(data.get('employeenumber', '')).rpartition('_r')
        base = head if sep and tail.isdigit() else data.get('employeenumber', '')
        if base != current_number:
            return True
        if current_company and data.get('companynumber') != current_company:
            return True
    return False


def find_transfer_active_mn_number():
    """Return the active employee number from the remark1 search (transfer source to terminate)."""
    return _find_active_mn_employee_number(
        _get_mn_records('search_by_employeeid_in_maconomy')
    )


def build_transfer_termination_payload():
    """Block the transfer source employee using the new hire date as the employment end date."""
    conf = rail.get_current_context()['dag_run'].conf
    end_date = _mn_date(conf.get('lastHireDate'))
    if not end_date:
        raise RuntimeError(
            f"Employee {conf.get('employeeNumber')}: cannot terminate the transfer "
            "source record without a lastHireDate"
        )
    return {
        'blocked': True,
        'dateendemployment': end_date,
    }


# ── Router conf builder ────────────────────────────────────────────────────

def build_router_conf(operation_type):
    """
    Merge person-details + employment-details XComs + MN search results
    into the child DAG conf dict.
    """
    conf = rail.get_current_context()['dag_run'].conf

    person_details = rail.result('get_person_details_from_ukgpro') or {}
    employment_details = (
        rail.result('get_employment_details_from_ukgpro') or {}
    )

    # Prefer re-fetched employment-details over trigger snapshot in conf
    last_hire_date = (
        employment_details.get('lastHireDate') or conf.get('lastHireDate')
    )
    date_of_termination = (
        employment_details.get('dateOfTermination')
        or conf.get('dateOfTermination')
    )
    supervisor_employee_number = (
        employment_details.get('supervisorEmployeeNumber')
        or conf.get('supervisorEmployeeNumber')
    )
    job_description = (
        employment_details.get('jobDescription') or conf.get('jobDescription')
    )
    org_level2_code = (
        employment_details.get('orgLevel2Code') or conf.get('orgLevel2Code')
    )
    org_level2_result = rail.result('get_org_level2_from_ukgpro') or {}
    org_level2_description = org_level2_result.get('description') or org_level2_code

    maconomy_company_number = resolve_maconomy_company_number()

    # Active Maconomy employee number from loop search (update/termination only)
    mn_employee_records = _get_mn_records('search_employee_in_maconomy')
    maconomy_employee_number = _find_active_mn_employee_number(mn_employee_records)

    return {
        'employeeID': conf.get('employeeID'),
        'employeeNumber': conf.get('employeeNumber'),
        'employeeStatusCode': conf.get('employeeStatusCode'),
        'workPhoneNumber': conf.get('workPhoneNumber'),
        'companyName': conf.get('companyName'),
        'companyID': conf.get('companyID'),
        'firstName': person_details.get('firstName'),
        'lastName': person_details.get('lastName'),
        'emailAddress': person_details.get('emailAddress'),
        'homePhone': person_details.get('homePhone'),
        'addressLine1': person_details.get('addressLine1'),
        'addressLine2': person_details.get('addressLine2'),
        'addressLine3': person_details.get('addressLine3'),
        'addressCity': person_details.get('addressCity'),
        'addressZipCode': person_details.get('addressZipCode'),
        'addressState': person_details.get('addressState'),
        'addressCountry': person_details.get('addressCountry'),
        'dateOfBirth': person_details.get('dateOfBirth'),
        'middleName': person_details.get('middleName'),
        'lastHireDate': last_hire_date,
        'dateOfTermination': date_of_termination,
        'supervisorEmployeeNumber': supervisor_employee_number,
        'jobDescription': job_description,
        'orgLevel2Code': org_level2_code,
        'orgLevel2Description': org_level2_description,
        'maconomy_company_number': maconomy_company_number,
        'maconomy_employee_number': maconomy_employee_number,
        'type': operation_type,
        'connections': conf.get('connections'),
    }


def build_create_conf():
    """Build conf for trigger_employee_create — handles new employee, rehire (suffix), and transfer."""
    conf = rail.get_current_context()['dag_run'].conf
    base_employee_number = conf.get('employeeNumber', '')

    records = _get_mn_records('search_employee_in_maconomy')
    if records:
        # Rehire: records exist but all blocked — next suffix is max(existing) + 1
        suffixes = []
        for r in records:
            number = str(r.get('data', {}).get('employeenumber', ''))
            _, sep, tail = number.rpartition('_r')
            if sep and tail.isdigit():
                suffixes.append(int(tail))
        next_suffix = max(suffixes, default=0) + 1
        if next_suffix > MAX_REHIRE_REVISIONS:
            raise RuntimeError(
                f"Employee {base_employee_number}: exhausted rehire suffixes "
                f"(_r1.._r{MAX_REHIRE_REVISIONS}); a new number would be invisible "
                "to the loop search"
            )
        create_employee_number = f'{base_employee_number}_r{next_suffix}'
    else:
        # New employee or transfer: use UKG employee number as-is
        create_employee_number = base_employee_number

    result = build_router_conf('add')
    result['employeeNumber'] = create_employee_number
    return result


def build_transfer_conf():
    """Build conf for transfer action — old MN record (by remark1) terminated, new UKG number created.
    maconomy_employee_number = old active MN number to terminate (None if already terminated)."""
    result = build_router_conf('transfer')
    # Loop search found nothing, so build_router_conf sets maconomy_employee_number=None.
    # Override with the old active record's number from the remark1 search.
    result['maconomy_employee_number'] = find_transfer_active_mn_number()
    return result


# ── Router error capture ───────────────────────────────────────────────────

def collect_triggered_dagrun_ids():
    """Collect dag run(s) from whichever trigger executed (create or update)."""
    dag_runs = []
    for task_id in [
        'trigger_employee_create',
        'trigger_employee_update',
    ]:
        try:
            result = rail.result(task_id)
            if result is not None:
                dag_runs.append(result)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    return dag_runs


def capture_router_dag_error(employee_number, fallback_error_message):
    """
    Aggregate child DAG errors first; fall back to this DAG's own error.
    Returns dict (non-raising) so the dispatcher's sensor stays green.
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
            f"Employee {employee_number} - sync failed: "
            f"{fallback_error_message}"
        )
    else:
        return None

    return {'error': error_message}


# ── Create DAG utilities ───────────────────────────────────────────────────

def resolve_supervisor_mn_number(task_id='search_supervisor_in_maconomy'):
    """Extract employeenumber from supervisor search XCom; None if not found."""
    records = _get_mn_records(task_id)
    if records:
        return records[0].get('data', {}).get('employeenumber')
    return None


def match_jobpricegroupnumber():
    """Return jobpricegroupnumber where description matches jobDescription."""
    conf = rail.get_current_context()['dag_run'].conf
    job_description = conf.get('jobDescription')
    for record in _get_mn_records('search_jobpricegroups_in_maconomy'):
        data = record.get('data', {})
        if data.get('description') == job_description:
            return data.get('jobpricegroupnumber')
    log.warning("No jobpricegroup matched jobDescription '%s'", job_description)
    return None


def match_employeecategorynumber():
    """Return employeecategorynumber where name matches jobDescription."""
    conf = rail.get_current_context()['dag_run'].conf
    job_description = conf.get('jobDescription')
    for record in _get_mn_records('search_employeecategory_in_maconomy'):
        data = record.get('data', {})
        if data.get('name') == job_description:
            return data.get('employeecategorynumber')
    log.warning("No employeecategory matched jobDescription '%s'", job_description)
    return None


def _mn_date(dt_str):
    """Strip time portion from ISO datetime for Maconomy date fields."""
    if not dt_str:
        return None
    return dt_str[:10]


def build_create_payload(
    instance,
    employee_defaults=None,
    *,
    supervisor_task_id='search_supervisor_in_maconomy',
):
    """Resolve lookups and build the full CREATE card payload."""
    from airflow.models import Variable  # pylint: disable=import-outside-toplevel
    conf = rail.get_current_context()['dag_run'].conf
    defaults = employee_defaults or {}

    supervisor_mn_number = resolve_supervisor_mn_number(supervisor_task_id)
    jobpricegroupnumber = match_jobpricegroupnumber()
    employeecategorynumber = match_employeecategorynumber()

    template_employee_number = Variable.get(
        f'ukgpro_mn_employee_sync_template_employee_number_{instance}',
        default_var=None
    )

    payload = {
        'employeenumber': conf.get('employeeNumber'),
        'remark1': conf.get('employeeID'),
        'firstname': conf.get('firstName'),
        'middlename': conf.get('middleName'),
        'lastname': conf.get('lastName'),
        'formalfirstname': conf.get('firstName'),
        'formalmiddlename': conf.get('middleName'),
        'formallastname': conf.get('lastName'),
        'telephone': conf.get('workPhoneNumber'),
        'mobilephone': conf.get('homePhone'),
        'country': conf.get('addressCountry') or None,
        'name2': conf.get('addressLine1'),
        'name3': conf.get('addressLine2'),
        'name4': conf.get('addressLine3'),
        'name5': conf.get('addressState'),
        'zipcode': conf.get('addressZipCode'),
        'postaldistrict': conf.get('addressCity'),
        'electronicmailaddress': conf.get('emailAddress'),
        'blocked': False,
        'salesemployee': defaults.get('salesemployee', True),
        'dateofbirth': _mn_date(conf.get('dateOfBirth')),
        'dateemployed': _mn_date(conf.get('lastHireDate')),
        'timesheetstartdate': _mn_date(conf.get('lastHireDate')),
        'companynumber': conf.get('maconomy_company_number'),
        'entityname': conf.get('orgLevel2Code'),
        'position': conf.get('jobDescription'),
        'maxworkingtimeperday': defaults.get('maxworkingtimeperday', 24),
        'standardbillingprice': defaults.get('standardbillingprice', 0),
        'mustusetimesheets': defaults.get('mustusetimesheets', True),
        'accountmanager': defaults.get('accountmanager', True),
    }

    if template_employee_number:
        payload['templateemployeenumber'] = template_employee_number
    if supervisor_mn_number:
        payload['superioremployee'] = supervisor_mn_number
        payload['absenceapprover'] = supervisor_mn_number
        payload['substitute1'] = supervisor_mn_number
    if jobpricegroupnumber:
        payload['jobpricegroupnumber'] = jobpricegroupnumber
    if employeecategorynumber:
        payload['primaryemployeecategorynumber'] = employeecategorynumber

    return {k: v for k, v in payload.items() if v is not None and v != '' and v != []}


def warn_supervisor_not_found_for_create():
    """Warn that supervisor was not found; employee will be created without."""
    conf = rail.get_current_context()['dag_run'].conf
    log.warning(
        "Supervisor not assigned for employee %s — supervisor %s not found "
        "in Maconomy. Employee will be created without supervisor.",
        conf.get('employeeNumber'), conf.get('supervisorEmployeeNumber'),
    )


def warn_supervisor_not_found_for_transfer():
    """Warn that supervisor was not found; transferred employee will be created without."""
    conf = rail.get_current_context()['dag_run'].conf
    log.warning(
        "Supervisor not assigned for transferred employee %s — supervisor %s not found "
        "in Maconomy. Employee will be created without supervisor.",
        conf.get('employeeNumber'), conf.get('supervisorEmployeeNumber'),
    )


def capture_create_error(employee_number, error_message):
    """Return create error dict for scheduler collection."""
    return {
        'error': (
            f"Employee {employee_number} - create failed: {error_message}"
        )
    }


# ── Update DAG utilities ───────────────────────────────────────────────────

def build_update_payload():
    """Build the UPDATE card payload (subset of fields)."""
    conf = rail.get_current_context()['dag_run'].conf
    supervisor_mn_number = resolve_supervisor_mn_number()

    payload = {
        'telephone': conf.get('workPhoneNumber'),
        'mobilephone': conf.get('homePhone'),
        'companynumber': conf.get('maconomy_company_number'),
        'country': conf.get('addressCountry') or None,
        'name2': conf.get('addressLine1'),
        'name3': conf.get('addressLine2'),
        'name4': conf.get('addressLine3'),
        'name5': conf.get('addressState'),
        'zipcode': conf.get('addressZipCode'),
        'postaldistrict': conf.get('addressCity'),
        'electronicmailaddress': conf.get('emailAddress'),
        'dateofbirth': _mn_date(conf.get('dateOfBirth')),
        'middlename': conf.get('middleName'),
        'entityname': conf.get('orgLevel2Code'),
        'position': conf.get('jobDescription'),
    }

    if supervisor_mn_number:
        payload['superioremployee'] = supervisor_mn_number
        payload['absenceapprover'] = supervisor_mn_number
        payload['substitute1'] = supervisor_mn_number

    return {k: v for k, v in payload.items() if v is not None and v != '' and v != []}


def build_termination_card_payload():
    """Build the TERMINATION card payload (block employee, set end date)."""
    conf = rail.get_current_context()['dag_run'].conf
    end_date = _mn_date(conf.get('dateOfTermination'))
    if not end_date:
        raise RuntimeError(
            f"Employee {conf.get('employeeNumber')}: cannot terminate without "
            "a dateOfTermination"
        )
    return {
        'blocked': True,
        'dateendemployment': end_date,
    }


def warn_supervisor_not_found_for_update():
    """Warn that supervisor was not found; employee will be updated without."""
    conf = rail.get_current_context()['dag_run'].conf
    log.warning(
        "Supervisor not assigned for employee %s — supervisor %s not found "
        "in Maconomy. Employee will be updated without supervisor.",
        conf.get('employeeNumber'), conf.get('supervisorEmployeeNumber'),
    )


def capture_update_error(employee_number, update_type, error_message):
    """Return update error dict for scheduler collection."""
    return {
        'error': (
            f"Employee {employee_number} ({update_type}) - "
            f"update failed: {error_message}"
        )
    }
