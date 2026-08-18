from datetime import datetime

import pendulum

from airflow.operators.python import get_current_context

from valleychildrens.user_import.utils import prereq_lookups, master_mapper

DATE_FORMAT = "%m-%d-%Y"

def get_task_state(task_id):
    ctx = get_current_context()
    ti = ctx['dag_run'].get_task_instance(task_id)
    return ti.state if ti else None

def to_date_struct(date_str, fmt=DATE_FORMAT):
    if not date_str:
        return None
    parsed = datetime.strptime(date_str, fmt)
    return {"year": parsed.year, "month": parsed.month, "day": parsed.day}

def today_date_struct(time_zone=None):
    """Today's date as a {year, month, day} struct, in the given timezone
    (e.g. config.pacific_timezone) so the effective date reflects the tenant's
    'today' rather than UTC."""
    now = pendulum.now(time_zone) if time_zone else pendulum.now()
    return {"year": now.year, "month": now.month, "day": now.day}

def _pick(item, *keys, default=""):
    """Return the first non-empty value found among `keys`.
    The conf builders are called both from the master (with raw input row keys
    like `emp_id`, `start_date`, `fte_total`) and from child DAGs (with the
    renamed keys like `employeeid`, `startdate`, `ftetotal`). _pick lets one
    helper signature serve both call sites.
    """
    for k in keys:
        v = item.get(k) if isinstance(item, dict) else None
        if v not in (None, ''):
            return v
    return default

def build_office_schedule_schedule(dag_run):
    """schedulePolicySchedule — populate the tenant office schedule URI from
    the master_mapper 'Schedule Type' (e.g. 'Mon-Fri (8 hrs/day)'). The
    scheduleTypeUri enum still defaults to 'shift' to match the Workato pattern."""
    office_uri = dag_run.conf.get('officescheduleuri')
    schedule_type_enum = dag_run.conf.get('scheduletypeenum') or 'urn:replicon:schedule-type:shift'
    return [{
        'schedulePolicy': {
            'officeScheduleUri': office_uri,
            'name': None,
            'officeSchedule': {'officeScheduleUri': office_uri, 'name': None},
            'scheduleTypeUri': schedule_type_enum,
        },
        'effectiveDate': None,
    }]

def build_service_center_schedule(dag_run):
    """serviceCenterSchedule — Workato shape (effectiveDate: null)."""
    uri = dag_run.conf.get('companyuri') or dag_run.conf.get('servicecenteruri')
    if not uri:
        return []
    return [{
        'serviceCenter': {'uri': uri, 'parentUri': None, 'name': None},
        'effectiveDate': None,
    }]

def build_department_group_schedule(dag_run):
    """departmentGroupSchedule — Workato omits parameterCorrelationId."""
    uri = dag_run.conf.get('departmenturi') or dag_run.conf.get('departmentgroupuri')
    if not uri:
        return []
    return [{
        'departmentGroup': {'uri': uri, 'parent': None, 'name': None},
        'effectiveDate': None,
    }]

def build_employee_type_group_schedule(dag_run):
    """employeeTypeGroupSchedule — Workato omits parameterCorrelationId."""
    uri = dag_run.conf.get('employeetypeuri') or dag_run.conf.get('employeetypegroupuri')
    if not uri:
        return []
    return [{
        'employeeTypeGroup': {'uri': uri, 'parent': None, 'name': None},
        'effectiveDate': None,
    }]

def build_timesheet_period_schedule(dag_run):
    """timesheetPeriodSchedule — Workato sends with effectiveDate: null."""
    uri = dag_run.conf.get('timesheetperioduri')
    if not uri:
        return []
    return [{
        'timesheetPeriod': {'uri': uri, 'name': None},
        'effectiveDate': None,
    }]

def build_timesheet_approval_path(dag_run):
    """timesheetApprovalPath — direct {uri, name} object, NOT a list."""
    uri = dag_run.conf.get('timesheetapprovaluri')
    if not uri:
        return None
    return {'uri': uri, 'name': None}

def build_timeoff_approval_path(dag_run):
    """timeOffApprovalPath — direct {uri, name} object, NOT a list."""
    uri = dag_run.conf.get('timeoffapprovaluri')
    if not uri:
        return None
    return {'uri': uri, 'name': None}

def build_policy_sets(dag_run):
    """policySets — Workato includes time off template + timesheet template + Expenses placeholder."""
    out = []
    timeoff_uri = dag_run.conf.get('timeofftemplateuri')
    timesheet_uri = dag_run.conf.get('timesheettemplateuri')
    if timeoff_uri:
        out.append({'uri': timeoff_uri, 'name': None})
    if timesheet_uri and timesheet_uri != timeoff_uri:
        out.append({'uri': timesheet_uri, 'name': None})
    out.append({'uri': None, 'name': 'Expenses'})
    return out

def build_policy_set_schedule_entries(dag_run, policy_uri, end_only=False):
    """Build the `policySetScheduleEntries` list that
    `/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule` accepts.
    Replicon expects a list of {effectiveDate, endDate, policySet:{uri,name}, policyUri}.
    `policyUri` is the policy-TYPE URI (e.g. urn:replicon:policy:time-off).
    end_only=True: produce an entry that just terminates the current schedule
    (used by payoutbalance). In that case policy_uri can be None and only
    endDate is populated.
    """
    effective = to_date_struct(_pick(dag_run.conf, 'effectivedate', 'effective_date', 'startdate'))
    if end_only:
        return [{
            'effectiveDate': effective,
            'endDate': effective,
            'policySet': None,
            'policyUri': 'urn:replicon:policy:time-off',
        }]
    if not policy_uri:
        return []
    return [{
        'effectiveDate': effective,
        'endDate': None,
        'policySet': {'uri': policy_uri, 'name': None},
        'policyUri': 'urn:replicon:policy:time-off',
    }]

def resolve_time_off_policy_uri(dag_run, default_policy_result, user_summary_result=None):
    """Policy precedence: explicit conf.policyuri → user's existing policy
    for this time off type → tenant default policy.

    Replicon holds the policy set URI under `.policySet.uri` on schedule
    entries (NOT `.policy.uri` — that field is the policy-type enum like
    'urn:replicon:policy:time-off').
    """
    explicit = dag_run.conf.get('policyuri') or dag_run.conf.get('policy_uri')
    if explicit:
        return explicit
    timeoff_type_uri = dag_run.conf.get('timeofftypeuri') or dag_run.conf.get('timeoffuri')
    if user_summary_result and timeoff_type_uri:
        for entry in user_summary_result or []:
            if not isinstance(entry, dict):
                continue
            entry_type = (entry.get('timeOffType') or {}).get('uri')
            if entry_type == timeoff_type_uri:
                schedule = entry.get('policySetSchedule') or []
                if schedule and isinstance(schedule[0], dict):
                    existing = (schedule[0].get('policySet') or {}).get('uri')
                    if existing:
                        return existing
    entries = []
    if isinstance(default_policy_result, list):
        entries = default_policy_result
    elif default_policy_result:
        entries = [default_policy_result]
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        uri = (entry.get('policySet') or {}).get('uri')
        if uri:
            return uri
    return None

def build_custom_field_values(dag_run):
    """customFieldValues — matches Workato successful payload:
      - FTE Total (numeric, value is STRING like "0.50"): {customField, text:null, date:null, dropDownOption:{}, number:"0.50"}
      - Employee Classification (dropdown): {customField, date:null, dropDownOption:{uri, name}}
      - CME Entitlement (dropdown): {customField, date:null, dropDownOption:{uri, name}}
    Order matches Workato output: FTE, Employee Classification, CME Entitlement.
    """
    out = []
    fte_field = dag_run.conf.get('fte_field_uri')
    fte_value = dag_run.conf.get('ftetotal')
    if fte_field and fte_value not in (None, ''):
        out.append({
            'customField': {'uri': fte_field, 'name': None, 'groupUri': None},
            'text': None,
            'date': None,
            'dropDownOption': {},
            'number': str(fte_value),
        })
    ec_field = dag_run.conf.get('employee_classification_field_uri')
    ec_option = dag_run.conf.get('employee_classification_option_uri')
    if ec_field and ec_option:
        out.append({
            'customField': {'uri': ec_field, 'name': None, 'groupUri': None},
            'date': None,
            'dropDownOption': {'uri': ec_option, 'name': None},
        })
    cme_field = dag_run.conf.get('cmeentitlement_field_uri')
    cme_option = dag_run.conf.get('cmeentitlement_option_uri')
    if cme_field and cme_option:
        out.append({
            'customField': {'uri': cme_field, 'name': None, 'groupUri': None},
            'date': None,
            'dropDownOption': {'uri': cme_option, 'name': None},
        })
    return out

def get_process_add_user_conf(item, config, log_id, supervisor_log_id):
    """Resolve URIs from master's prereq tasks and the master_mapper lookup
    table, then pass everything the child needs via dag_run.conf. Called from
    the master's TriggerDagRunForEachItemOperator, where rail.result() of
    prereq tasks is in scope. log_id/supervisor_log_id are the master-created
    log artifacts so all children write into the same log."""
    mapper_values = master_mapper.lookup_all_fields(
        config.USER_IMPORT_MAPPER,
        item.get("department"),
        item.get("company"),
        item.get("employee_type"),
        item.get("cme_entitlement"),
    )
    return {
        "log_id": log_id,
        "supervisor_log_id": supervisor_log_id,
        "employeeid": item["emp_id"],
        "firstname": item["first_name"],
        "lastname": item["last_name"],
        "employeetype": item.get("employee_type", ""),
        "company": item.get("company", ""),
        "department": item.get("department", ""),
        "cmeentitlement": item.get("cme_entitlement", ""),
        "startdate": item.get("start_date", ""),
        "adjustedstartdate": item.get("adjusted_start_date", ""),
        "enddate": item.get("end_date", ""),
        "ftetotal": item.get("fte_total", ""),
        "supname": item.get("sup_name", ""),
        "supid": item.get("sup_id", ""),
        "loginname": item.get("login_name", ""),
        "email": item["email"],
        "companykey": config.company_key,
        "employeetypeuri": prereq_lookups.employee_type_uri(item.get("employee_type")),
        "departmenturi": prereq_lookups.department_uri(item.get("department")),
        "companyuri": prereq_lookups.company_uri(item.get("company")),
        "supervisorpermissionuri": prereq_lookups.supervisor_permission_uri(),
        "userpermissionuri": prereq_lookups.basic_user_permission_uri(),
        "cmeentitlement_field_uri": prereq_lookups.cme_entitlement_field_uri(),
        "cmeentitlement_option_uri": prereq_lookups.cme_entitlement_option_uri(item.get("cme_entitlement")),
        "fte_field_uri": prereq_lookups.fte_field_uri(),
        "fte_effective_date_field_uri": prereq_lookups.fte_effective_date_field_uri(),
        "employee_classification_field_uri": prereq_lookups.employee_classification_field_uri(),
        "timesheettemplateuri": prereq_lookups.timesheet_template_uri(mapper_values.get("Timesheet Template")),
        "timesheetapprovaluri": prereq_lookups.timesheet_approval_uri(mapper_values.get("Timesheet Approval")),
        "timesheetperioduri": prereq_lookups.timesheet_period_uri(mapper_values.get("Timesheet Period")),
        "timeofftemplateuri": prereq_lookups.timeoff_template_uri(mapper_values.get("Time Off Template")),
        "timeoffapprovaluri": prereq_lookups.timeoff_approval_uri(mapper_values.get("Timeoff Approval")),
        "holidaycalendaruri": prereq_lookups.holiday_calendar_uri(mapper_values.get("Holiday Calender")),
        "officescheduleuri": prereq_lookups.office_schedule_uri(mapper_values.get("Schedule Type")),
        "scheduletypeenum": prereq_lookups.schedule_type_enum_uri(mapper_values.get("Schedule Type"))
            or 'urn:replicon:schedule-type:shift',
        "authenticationtypeuri": prereq_lookups.authentication_type_uri(mapper_values.get("Authentication Type")),
        "workweekstartdayuri": prereq_lookups.work_week_start_day_uri(mapper_values.get("Work Week")),
        "activities": prereq_lookups.activities_for_department(
            config.ACTIVITY_DEPARTMENT_MAPPER, item.get("department")),
        "employee_classification_value": mapper_values.get("Employee Classification"),
        "employee_classification_option_uri": prereq_lookups.employee_classification_option_for_value(
            mapper_values.get("Employee Classification")),
        "_mapper_values": mapper_values,
    }

def get_process_user_update_conf(item, config, log_id, supervisor_log_id):
    payload = get_process_add_user_conf(item, config, log_id, supervisor_log_id)
    payload["useruri"] = item.get("user_uri", "")
    payload["existing_login_name"] = item.get("existing_login_name", "")
    payload["existing_end_date"] = item.get("existing_end_date", "")
    payload["existing_user_status"] = item.get("existing_user_status", "")
    return payload

def get_process_supervisor_assignment_conf(item, config, supervisor_log_id):
    return {
        "supervisor_log_id": supervisor_log_id,
        "employeeid": _pick(item, "emp_id", "employeeid", "employee_id"),
        "supid": _pick(item, "sup_id", "supid"),
        "supname": _pick(item, "sup_name", "supname"),
        "useruri": _pick(item, "user_uri", "useruri"),
        "supervisorpermissionuri": _pick(item, "supervisor_permission_uri", "supervisorpermissionuri")
            or prereq_lookups.supervisor_permission_uri(),
        "companykey": config.company_key,
    }

def get_process_timeoff_add_new_user_conf(item, config, log_id):
    return {
        "log_id": log_id,
        "employeeid": _pick(item, "emp_id", "employeeid", "employee_id"),
        "useruri": _pick(item, "user_uri", "useruri"),
        "startdate": _pick(item, "start_date", "startdate"),
        "ftetotal": _pick(item, "fte_total", "ftetotal"),
        "timeofftypes": _pick(item.get("_mapper_values"), "Time Off Types"),
        "companykey": config.company_key,
    }

def get_process_time_off_policy_add_pto_conf(item, config, log_id):
    return {
        "log_id": log_id,
        "useruri": _pick(item, "user_uri", "useruri"),
        "timeofftypeuri": _pick(item, "time_off_type_uri", "timeofftypeuri"),
        "policyuri": _pick(item, "policy_uri", "policyuri"),
        "effectivedate": _pick(item, "effective_date", "effectivedate", "startdate"),
        "ftetotal": _pick(item, "fte_total", "ftetotal"),
        "companykey": config.company_key,
    }

def get_process_time_off_policy_update_on_fte_change_conf(item, config, log_id):
    return {
        "log_id": log_id,
        "useruri": _pick(item, "user_uri", "useruri"),
        "timeofftypeuri": _pick(item, "time_off_type_uri", "timeofftypeuri"),
        "policyuri": _pick(item, "policy_uri", "policyuri"),
        "effectivedate": _pick(item, "effective_date", "effectivedate", "startdate"),
        "ftetotal": _pick(item, "fte_total", "ftetotal"),
        "previous_ftetotal": _pick(item, "previous_fte_total", "previous_ftetotal"),
        "companykey": config.company_key,
    }

def get_process_timeoff_policy_payoutbalance_conf(item, config, log_id):
    return {
        "log_id": log_id,
        "useruri": _pick(item, "user_uri", "useruri"),
        "timeofftypeuri": _pick(item, "time_off_type_uri", "timeofftypeuri"),
        "balance": _pick(item, "balance"),
        "effectivedate": _pick(item, "effective_date", "effectivedate", "startdate"),
        "companykey": config.company_key,
    }

def get_process_update_user_time_off_assign_conf(item, config, log_id):
    """Conf for the update/rehire time-off-assign child DAGs. Reads from an
    already-transformed conf (employeeid/useruri/...) or a raw collection row
    (emp_id/user_uri/...) via _pick. Derives timeofftypes from a top-level key
    or the master_mapper's 'Time Off Types' carried in _mapper_values."""
    return {
        "log_id": log_id,
        "employeeid": _pick(item, "emp_id", "employeeid", "employee_id"),
        "useruri": _pick(item, "user_uri", "useruri"),
        "startdate": _pick(item, "start_date", "startdate"),
        "ftetotal": _pick(item, "fte_total", "ftetotal"),
        "previous_ftetotal": _pick(item, "previous_fte_total", "previous_ftetotal"),
        "timeofftypes": _pick(item, "timeofftypes")
            or _pick(item.get("_mapper_values"), "Time Off Types"),
        "companykey": config.company_key,
    }

def get_mandatory_fields_exception_message(item):
    """Used on both raw input rows (emp_id) and renamed confs (employeeid)."""
    missing = []
    pairs = [
        ("employee_id", ("emp_id", "employeeid", "employee_id")),
        ("first_name",  ("first_name", "firstname")),
        ("last_name",   ("last_name", "lastname")),
        ("email",       ("email", "emailAddress")),
        ("login_name",  ("login_name", "loginname")),
    ]
    for label, keys in pairs:
        if not _pick(item, *keys):
            missing.append(label)
    return f"Missing required fields: {', '.join(missing)}" if missing else "Missing mandatory fields"

def test_valid_fields(dag_run):
    """Accept either raw or renamed key for each required field."""
    pairs = [
        ("emp_id", "employeeid", "employee_id"),
        ("first_name", "firstname"),
        ("last_name", "lastname"),
        ("email", "emailAddress"),
    ]
    return all(_pick(dag_run.conf, *keys) for keys in pairs)

