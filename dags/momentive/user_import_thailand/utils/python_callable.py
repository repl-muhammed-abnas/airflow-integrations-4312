# pylint: disable=line-too-long
from datetime import datetime
from pendulum import now
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_thailand.config import time_zone
from momentive.user_import_thailand.mappers.momentive_thailand_mapper import mapper_value


def split_date_string(date_str, split_type='string'):
    """Split a 'YYYY-MM-DD' string into day/month/year parts.

    split_type='int' returns integers, 'datetime' returns the raw int components,
    otherwise zero-padded strings.
    """
    date = datetime.strptime(date_str, "%Y-%m-%d")
    if split_type == 'datetime':
        return {
            "day": date.day,
            "month": date.month,
            "year": date.year
        }
    if split_type == 'int':
        return {
            "day": int(date.strftime("%d")),
            "month": int(date.strftime("%m")),
            "year": int(date.strftime("%Y"))
        }

    return {
        "day": date.strftime("%d"),
        "month": date.strftime("%m"),
        "year": date.strftime("%Y")
    }

def mapper_keys(dag_run, costcenter_key='cost_center'):
    """Derived lookup keys for the Momentive_Thailand_Mapper (recipe [13]-[25]).
    `costcenter_key` is the conf key holding the cost center: 'cost_center' for the
    update flow, 'costcenter' for the add flow.
    """
    c = dag_run.conf
    is_contingent = c.get('Worker_Type') == "Contingent Worker"
    return {
        'exemptstatus': "Yes" if "1" in str(c.get('Exemption_Status') or "") else "No",
        'shift': c['Work_Shift'] if c.get('Work_Shift') else "Null",
        'gender': "Any" if is_contingent else c.get('Gender'),
        'cost_center': "Any" if (is_contingent or c.get('Exemption_Status') == "1")
        else ("T30930" if "T30930" in str(c.get(costcenter_key) or "") else "Any"),
    }


def current_udf_values():
    """Current custom-field values for the user (from get_user_data BulkGetUsers3),
    keyed for the update mismatch checks, plus the field URIs (from GetAllCustomFields).
    Date values are returned as 'YYYY/M/D' to mirror Replicon's stored format; text
    values are lowercased for case-insensitive comparison.
    """
    details = (rail.result('get_user_data') or [{}])[0].get('userDetails', {}) or {}
    by_name = {}
    for item in (details.get('customFieldValues') or []):
        name = (item.get('customField') or {}).get('displayText')
        if not name:
            continue
        if item.get('textValue') is not None:
            by_name[name] = item.get('textValue') or ''
        elif item.get('numericValue') is not None:
            by_name[name] = str(item.get('numericValue'))
        elif item.get('dateValue'):
            d = item['dateValue']
            by_name[name] = f"{d.get('year')}/{d.get('month')}/{d.get('day')}"

    def low(nm):
        v = by_name.get(nm)
        return v.lower() if isinstance(v, str) else v

    def uri(nm):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fields'), 'displayText', nm, 'uri', '')

    return {
        'dob': by_name.get('Date of Birth'), 'dob_uri': uri('Date of Birth'),
        'gender': low('Gender'), 'gender_uri': uri('Gender'),
        'hrm': low('HRM'), 'hrm_uri': uri('HRM'),
        'title': low('Title'), 'title_uri': uri('Title'),
        'yearsofservice': by_name.get('Years of Service'), 'yearsofservice_uri': uri('Years of Service'),
    }


def custom_field_uri(display_text):
    """Recipe [58]: resolve a user custom-field URI by display text from get_custom_fields.
    Returns '' when the field does not exist in the instance, which the callers use to skip
    the update instead of posting an empty customFieldUri.
    """
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_custom_fields'), 'displayText', display_text, 'uri', '')


def _keys():
    return rail.result('compute_mapper_keys')


def employee_type_name(dag_run):
    return mapper_value('Employee Type', workertype=dag_run.conf['Worker_Type'], location='Null',
                        exemptstatus=_keys()['exemptstatus'], shift=_keys()['shift'], gender='Any', cost_center='Any')


def approvalpath_name(dag_run):
    return "System Approval" if dag_run.conf.get('Exemption_Status') == "1" else "Supervisor"


def holiday_name(dag_run):
    return mapper_value('Holiday Calendar', workertype='Any', location=dag_run.conf['Location'],
                        exemptstatus='Any', shift=_keys()['shift'], gender='Any', cost_center='Any')


def timesheet_template_name(dag_run):
    return mapper_value('Timesheet Template', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                        exemptstatus=_keys()['exemptstatus'], shift=_keys()['shift'], gender=_keys()['gender'], cost_center=_keys()['cost_center'])


def payrule_name(dag_run):
    return mapper_value('Payrule', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                        exemptstatus=_keys()['exemptstatus'], shift=_keys()['shift'], gender=_keys()['gender'], cost_center=_keys()['cost_center'])


def schedule_name(dag_run):
    return mapper_value('Schedule', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                        exemptstatus=_keys()['exemptstatus'], shift=_keys()['shift'], gender=_keys()['gender'], cost_center=_keys()['cost_center'])


def activity_name(dag_run):
    return mapper_value('Activity', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                        exemptstatus=_keys()['exemptstatus'], shift=_keys()['shift'], gender=_keys()['gender'], cost_center=_keys()['cost_center'])


def timeoff_types(dag_run):
    return mapper_value('Time off types', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                        exemptstatus=_keys()['exemptstatus'], shift=_keys()['shift'], gender=_keys()['gender'], cost_center=_keys()['cost_center'])


def supervisor_details():
    """{useruri, enabled} for the supervisor matched by employee id."""
    row = rail.result('search_supervisor')[0]
    return {'useruri': row['cells'][1]['uri'], 'enabled': row['cells'][2]['boolValue']}


# --------------------------------------------------------------------------- #
# Boolean computations behind IfOperator tests (the IfOperator test stays a
# Jinja `{{ result('<task>') | is_truthy }}`; the Python logic lives here).
# --------------------------------------------------------------------------- #

def hiredate_differs_from_startdate(dag_run):
    if not dag_run.conf.get('Hire_Date'):
        return False
    employment = ((rail.result('get_user_data') or [{}])[0].get('userDetails', {}) or {}).get('employmentDateRange') or {}
    return split_date_string(dag_run.conf['Hire_Date'], 'datetime') != employment.get('startDate')


def terminationdate_present_and_differs(dag_run):
    if not dag_run.conf.get('Termination_Date'):
        return False
    employment = ((rail.result('get_user_data') or [{}])[0].get('userDetails', {}) or {}).get('employmentDateRange') or {}
    return split_date_string(dag_run.conf['Termination_Date'], 'datetime') != employment.get('endDate')


def dob_present_and_mismatch(dag_run):
    incoming = dag_run.conf.get('CF_Date_of_Birth_MM_DD_YYYY')
    if not incoming or '-' not in incoming:
        return False
    current = rail.result('get_user_udf_values')['dob']
    if not current:
        return True
    return datetime.strptime(incoming, '%Y-%m-%d') != datetime.strptime(current, '%Y/%m/%d')


def timeofftrigger_flag():
    return rail.get_dag_run_var('timeofftrigger') == 'true'


def current_payrule_name():
    """Currently-assigned payrule name (latest schedule entry), for the payrule mismatch gate.
    Reads GetPayRuleScriptAssignmentScheduleForUser; returns '' when none assigned.
    """
    schedule = rail.result('get_payrule_assignment_schedule') or []
    entries = schedule if isinstance(schedule, list) else (schedule.get('scheduleEntries') or [])
    names = [((entry.get('payRuleScript') or {}).get('displayText') or '') for entry in entries]
    names = [name for name in names if name]
    return names[-1] if names else ''


def current_supervisor_uri():
    """The user's current supervisor user-URI, matched by the supervisor displayText from
    the UserListService GetData (cells[2]) against the BulkGetUsers3 supervisorAssignmentSchedule.
    Returns '' when none. Used to fetch the current supervisor's employeeId for the #108 gate.
    """
    rows = (rail.result('getdata_sup_emp_grp_dept_grp') or {}).get('rows') or []
    cells = rows[0]['cells'] if rows else []
    sup_text = cells[2].get('textValue') if len(cells) > 2 else None
    schedule = ((rail.result('get_user_data') or [{}])[0].get('userDetails', {}) or {}).get('supervisorAssignmentSchedule') or []
    for entry in schedule:
        supervisor = entry.get('supervisor') or {}
        if supervisor.get('displayText') == sup_text:
            return ((supervisor.get('user') or {}).get('uri')) or ''
    return ''


def build_schedule_entries(dag_run):
    """Recipe #440-#471: rebuild the schedule policy entries.

    Preserves the user's current schedule entries (blank-dated kept with null effectiveDate;
    dated entries kept unless their effective date equals the change date), then appends the
    new target entry. If no current entry survives, returns a single entry with null
    effectiveDate. Shift vs office schedule is decided by the mapped target.
    """
    current = (rail.result('get_user_data_14') or [{}])[0].get('schedulePolicies') or []
    target_name = rail.result('log_schedule_tobeassigned') or ''
    target_uri = rail.result('get_req_schedule_script') or ''
    is_shift = 'Shift' in target_name
    eff_raw = dag_run.conf.get('Work_Shift_Change_Effective_Date') or str(now(tz=time_zone).date())
    change = split_date_string(eff_raw, 'datetime')

    kept = []
    for entry in current:
        schedule_type = entry.get('scheduleTypeUri') or ''
        eff = entry.get('effectiveDate') or {}
        if 'shift' in schedule_type:
            policy = {"officeScheduleUri": None, "scheduleTypeUri": schedule_type, "name": None, "officeSchedule": None}
        else:
            policy = {"officeScheduleUri": (entry.get('officeSchedule') or {}).get('uri'), "scheduleTypeUri": schedule_type, "name": None, "officeSchedule": None}
        if not eff.get('day'):
            kept.append({"schedulePolicy": policy, "effectiveDate": None})
        elif (eff.get('year'), eff.get('month'), eff.get('day')) != (change['year'], change['month'], change['day']):
            kept.append({"schedulePolicy": policy, "effectiveDate": {"year": eff['year'], "month": eff['month'], "day": eff['day']}})

    if is_shift:
        new_policy = {"officeScheduleUri": None, "scheduleTypeUri": "urn:replicon:schedule-type:shift", "name": None, "officeSchedule": None}
    else:
        new_policy = {"officeScheduleUri": target_uri, "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule", "name": None, "officeSchedule": None}

    if not kept:
        return [{"schedulePolicy": new_policy, "effectiveDate": None}]
    kept.append({"schedulePolicy": new_policy, "effectiveDate": {"year": change['year'], "month": change['month'], "day": change['day']}})
    return kept


# --------------------------------------------------------------------------- #
# update_user_child_dag (Japan-structured port) — PythonOperator callables
# Thailand conf keys are capitalized; UDF/group URIs are resolved in-DAG.
# --------------------------------------------------------------------------- #

def get_input_validationlog(dag_run):
    """Validate that the mandatory update fields are present in conf."""
    c = dag_run.conf
    exception_list = []
    if not c.get('User_ID'):
        exception_list.append('Login name not present')
    if not c.get('First_Name'):
        exception_list.append('First_Name not present')
    if not c.get('Last_Name'):
        exception_list.append('Last_Name not present')
    if not c.get('Hire_Date'):
        exception_list.append('Hire date not present')
    if not c.get('Email_Address'):
        exception_list.append('Email_Address not present')
    if not c.get('Exemption_Status'):
        exception_list.append('Excemption Status not present')
    if not c.get('Worker_Type'):
        exception_list.append('Worker type not present')
    if not c.get('Location'):
        exception_list.append('Department (location) not present')
    if not c.get('Active'):
        exception_list.append('Employee status not present')
    if not c.get('Manager_ID'):
        exception_list.append('Manager ID not present')
    if not c.get('Country'):
        exception_list.append('Country not present')

    if exception_list:
        return {'exc_present': True, 'exc_value': ','.join(exception_list)}
    return {'exc_present': False, 'exc_value': ''}


def validate_hiredate_startdate(dag_run):
    """True when the incoming Hire_Date equals the user's current employment start
    date (Japan gate: equal -> skip the remove-end-date/rehire-date branch).
    Reads get_user_data_14.
    """
    start = rail.result('get_user_data_14')[0]['userDetails']['employmentDateRange']['startDate']
    incoming = datetime.strptime(dag_run.conf['Hire_Date'], "%Y-%m-%d")
    current = datetime.strptime(
        f"{start['year']}-{start['month']}-{start['day']}", "%Y-%m-%d")
    return bool(current == incoming)


def validate_terminationdate_enddate(dag_run):
    """True when Termination_Date is present AND differs from the existing end date
    (01/01/2099 sentinel when the user has no end date). Reads get_user_data_14.
    """
    enddate = datetime.strptime('2099-01-01', "%Y-%m-%d")
    userend_date = rail.result('get_user_data_14')[0]['userDetails']['employmentDateRange']['endDate']
    if dag_run.conf.get('Termination_Date'):
        if userend_date and 'day' in userend_date:
            enddate = datetime.strptime(
                f"{userend_date['year']}-{userend_date['month']}-{userend_date['day']}", "%Y-%m-%d")
        if enddate != datetime.strptime(dag_run.conf['Termination_Date'], "%Y-%m-%d"):
            return True
    return False


def get_udf_values_from_userdetails():
    """Current custom-field VALUES for the Thailand user (from get_user_data_14).
    Thailand has only 5 UDFs: Date of Birth, Title, Years of Service, HRM, Gender.
    Text values are lowercased for case-insensitive mismatch checks; DOB raw.
    """
    user_customfield = rail.result('get_user_data_14')[0]['userDetails']['customFieldValues']

    def _get_raw(display_text):
        return rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', display_text, 'text', '') or ''

    def _get_lower(display_text):
        return _get_raw(display_text).lower()

    return {
        'dob': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Date of Birth', 'text', ''),
        # Recipe [55] compares Gender case-SENSITIVELY (no downcase); Title/YoS/HRM
        # use .downcase in the recipe, so those stay lowercased.
        'gender': _get_raw('Gender'),
        'hrm': _get_lower('HRM'),
        'title': _get_lower('Title'),
        'yearsofservice': _get_lower('Years of Service'),
    }


def compare_dates_to_today(dag_run):
    """For each change-effective-date field, True when it equals today (tenant tz).
    Thailand conf keys (capitalized); cost-center change date is 'eff_cost_center'.
    """
    today = str(now(tz=time_zone).date())

    def eq(key):
        v = dag_run.conf.get(key)
        return bool(v) and str(v) == today

    return {
        'exemption_eff_date': eq('Exemption_Eff_Date'),
        'work_shift_change_effective_date': eq('Work_Shift_Change_Effective_Date'),
        'effective_date_of_workertype': eq('Effective_Date_of_Worker_Type'),
        'cf_lrv_location_change_effective_date': eq('CF_LRV_Location_Change_Effective_Date'),
        'cost_center_change_effective_date': eq('eff_cost_center'),
    }


def get_startday_of_nexttimesheet():
    """Today's date as 'YYYY-MM-DD' (tenant tz)."""
    return str(now(tz=time_zone).date())


def build_user_list_item():
    """Existing-user row appended to the master 'userlist' (SetVariableOperator value).
    UserListService GetData omits keys for empty cells (list-type:null cells have no
    textValue; empty date cells have no dateValue) — use .get() and normalize dateValue
    dicts to 'YYYY-MM-DD' strings the downstream date splitter (split_date_string) parses.
    """
    cells = rail.result('foreach_search_users_33')['cells']

    def cell_date(cell):
        date_value = cell.get('dateValue')
        if cell.get('textValue') and date_value:
            return f"{date_value['year']}-{date_value['month']:02d}-{date_value['day']:02d}"
        return None

    return {
        "username": cells[0]['textValue'].lower() if cells[0].get('textValue') else None,
        "useruri": cells[0].get('uri'),
        "status": cells[3].get('boolValue'),
        "enddate": cell_date(cells[1]),
        "startdate": cell_date(cells[2]),
        "employee_type": cells[4].get('textValue') if len(cells) > 4 and cells[4].get('textValue') else None,
    }


def get_userdata_list_for_managerid(response, dag_run):
    """Filter a UserListService GetData response (employee-id, login-name columns)
    to rows whose employee-id cell (cells[0]) matches Manager_ID.
    Returns [{uri, loginname, managerid_txt}].
    """
    manager_id = dag_run.conf['Manager_ID']
    rows = [{
        'uri': d['cells'][1]['uri'],
        'loginname': d['cells'][1]['textValue'],
        'managerid_txt': d['cells'][0]
    } for d in response['rows']]
    return list(filter(
        lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == manager_id, rows))


def search_userdata_list_for_supervisorloginname(response, dag_run):
    """Filter a UserListService GetData response (employee-id, login-name, enabled
    columns) to rows whose employee-id cell (cells[0]) matches supervisorloginname.
    Returns [{uri, loginname, managerid_txt}]; [] when none match.
    Recipe steps 8-9: locate the supervisor user(s) by their employee id.
    """
    if list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['supervisorloginname'], response['rows'])):
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['supervisorloginname'], list(map(
            lambda d: {
                'uri': d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt': d['cells'][0]
            }, response['rows']))))
    return []


def existing_users_for_supervisor_email(response, dag_run):
    target = (dag_run.conf.get('sup_email') or '').strip().lower()
    if not target:
        return []
    return [
        row['cells'][1].get('uri')
        for row in (response.get('rows') or [])
        if (row['cells'][1].get('textValue') or '').strip().lower() == target
    ]


def get_exceptions():
    """Concatenated supervisor exception/detail string appended to the user's import
    log entry. Recipe: multiple-EMP-id match, disabled supervisor, or a missing
    foreign-supervisor id each contribute their message.
    """
    return ("Supervisor not assigned sincemultiple users found with same EMP id" if len(rail.result('search_for_user_with_empid') or []) > 1 else '') + (
        rail.result('log_supervisor_disabled') if rail.result('log_supervisor_disabled') else '') + (
            rail.result('log_foreign_supervisor_not_received') if rail.result('log_foreign_supervisor_not_received') else '')


def get_supervisor_status_escalation():
    """Recipe steps 12/29: only multiple-match and disabled-supervisor escalate the
    entry status to Exception; created/not-received keep the original status.
    """
    return ("Supervisor not assigned sincemultiple users found with same EMP id" if len(rail.result('search_for_user_with_empid') or []) > 1 else '') + (
        rail.result('log_supervisor_disabled') or '')


def get_current_group_display_text(group_key, item_key):
    """First entry's displayText from GetEffectiveUserGroupMembership, '' when absent.
    Reads get_effectiveusergroupmembership.
    """
    membership = rail.result('get_effectiveusergroupmembership') or {}
    groups = membership.get(group_key) or []
    if not groups:
        return ''
    inner = ((groups[0] or {}).get(item_key) or {}).get(item_key) or {}
    return inner.get('displayText') or ''


def _build_group_list(task_id):
    """Flatten a *ListService GetData response into [{name, uri, fullpath}]."""
    res = rail.result(task_id) or {}
    out = []
    for r in res.get('rows', []):
        cells = r['cells'][1]['cellCollection'] if r['cells'][1].get('cellCollection') else []
        out.append({
            'name': r['cells'][0]['textValue'],
            'uri': r['cells'][0]['uri'],
            'fullpath': " / ".join(c.get('textValue', '') for c in cells),
        })
    return out


def build_costcenter_list():
    """Flatten cost-center GetData rows into [{name, uri, fullpath}]. Reads get_costcenter_group_data."""
    return _build_group_list('get_costcenter_group_data')


def build_legalentity_list():
    """Flatten division GetData rows into [{name, uri, fullpath}]. Reads get_division_group_data."""
    return _build_group_list('get_division_group_data')


def build_servicecenter_list():
    """Flatten service-center GetData rows into [{name, uri, fullpath}]. Reads get_servicecenter_group_data."""
    return _build_group_list('get_servicecenter_group_data')


def get_status_and_details_for_update(dag_run):
    """Recipe [486]: status escalates to "Exception" ONLY when one of the 4
    status loggers fired (DOB-invalid, supervisor-multiple, emp-type-not-found,
    dept-group-not-found -> status_exception_log). The other "not found"
    messages (timesheet-template/paygroup/cost-center/legal-entity ->
    exception_log) are details-only and report Success (recipe col5 vs col6).
    details = all exception messages + field-change entries, or
    "No field changes were received".
    """
    has_log_entries = ','.join(list(
        map(lambda v: v['properties']['value'], rail.load_all_records(rail.result('log_entries')))))
    status_exceptions = ','.join(list(map(
        lambda v: v['properties']['value'], rail.load_all_records(rail.result('status_exception_log')))))
    other_exceptions = ','.join(list(map(
        lambda v: v['properties']['value'], rail.load_all_records(rail.result('exception_log')))))
    has_exception_message = ','.join([m for m in [status_exceptions, other_exceptions] if m])
    message = "Exception" if status_exceptions else "Success"
    if has_log_entries:
        details = (has_exception_message + ',' + has_log_entries) if has_exception_message else has_log_entries
    else:
        details = (has_exception_message + ',' if has_exception_message else '') + "No field changes were received"
    return {
        "jobid": dag_run.conf['parentjobid'],
        "userid": dag_run.conf['User_ID'],
        "username": dag_run.conf['First_Name'] + " " + dag_run.conf['Last_Name'],
        "action": "Update",
        "status": message,
        'details': details,
        "childjobid": get_dagrun_ecid(dag_run),
    }


# --------------------------------------------------------------------------- #
# update_user_timeoff_assign_dag — PythonOperator callables
# Leaf child: assign the incoming time-off types and rebuild the annual-leave
# policy-set schedule. Conf keys are capitalized; timeofftypes is pipe-delimited.
# --------------------------------------------------------------------------- #

def timeofftype_names_list(dag_run):
    """Recipe [23]/[25]: split the pipe-delimited `timeofftypes` conf into trimmed names."""
    raw = dag_run.conf.get('timeofftypes')
    return [item.strip() for item in raw.split("|")] if raw else []


def final_timeofftype_uris(dag_run):
    """Recipe [27]/[31]: map each incoming time-off type display name to its enabled URI
    (from get_enabled_timeoff_types displayText -> uri) and return the URI list.
    """
    names = [item.strip() for item in dag_run.conf['timeofftypes'].split("|")] if dag_run.conf.get('timeofftypes') else []
    uris = []
    for name in names:
        uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_enabled_timeoff_types'), 'displayText', name, 'uri', '')
        if uri:
            uris.append(uri)
    return uris


def is_annual_leave_type_requested(dag_run):
    """Recipe [38]/[39]: True when the current loop annual-leave type display name
    (foreach_annual_leave_type result) is in the incoming pipe-delimited set.
    """
    names = [item.strip() for item in dag_run.conf['timeofftypes'].split("|")] if dag_run.conf.get('timeofftypes') else []
    return rail.result('foreach_annual_leave_type') in names


def annual_leave_enabled_uri():
    """Recipe [43]: the enabled URI for the current loop annual-leave type
    (get_enabled_timeoff_types displayText -> uri).
    """
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_enabled_timeoff_types'), 'displayText', rail.result('foreach_annual_leave_type'), 'uri', '')


def annual_leave_assigned_uri():
    """Recipe [40]: the user's currently-assigned URI for the current loop annual-leave
    type, from get_assigned_timeoff_types d[0].timeOffTypeAssignmentsDetails.timeOffTypes
    (displayText == type). Returns '' when not assigned.
    """
    assigned = rail.result('get_assigned_timeoff_types') or []
    if not assigned:
        return ''
    timeoff_types = ((assigned[0].get('timeOffTypeAssignmentsDetails') or {}).get('timeOffTypes')) or []
    return rail.find_first_by_attr_and_get_attr(timeoff_types, 'displayText', rail.result('foreach_annual_leave_type'), 'uri', '')


def not_assigned_or_rehire(dag_run):
    """Recipe [41]: rebuild the policy when the current loop type is not currently
    assigned to the user OR the incoming record is a rehire.
    """
    return (not annual_leave_assigned_uri()) or (dag_run.conf.get('rehire') == 'rehire')
