# pylint: disable=line-too-long
from datetime import datetime
import rail


def user_import_data(item):
    """One canonical CSV row from a raw Workday report record (spaced display-label keys,
    same shared other-countries report as UAE/Japan; the Japan-flag column exists in the
    report but is unused for France)."""
    return [
        item['User ID'] if item['User ID'] else '',
        item['Worker reference employee ID'] if item['Worker reference employee ID'] else '',
        item['Email address'] if item['Email address'] else '',
        item['First name'] if item['First name'] else '',
        item['Last name'] if item['Last name'] else '',
        item['Worker type'] if item['Worker type'] else '',
        item['Effective date of worker type'] if item['Effective date of worker type'] else '',
        item['Exemption status'] if item['Exemption status'] else '',
        item['Exemption eff date'] if item['Exemption eff date'] else '',
        item['Gender'] if item['Gender'] else '',
        item['Hire date'] if item['Hire date'] else '',
        item['Termination date'] if item['Termination date'] else '',
        item['Active'] if item['Active'] else '',
        item['Function'] if item['Function'] else '',
        item['Function change effective date'] if item['Function change effective date'] else '',
        item['Business title'] if item['Business title'] else '',
        item['CF LRV business title change eff date'] if item['CF LRV business title change eff date'] else '',
        item['Field HR'] if item['Field HR'] else '',
        item['Manager ID'] if item['Manager ID'] else '',
        item['Effective date of manager change'] if item['Effective date of manager change'] else '',
        item['Work shift'] if item['Work shift'] else '',
        item['Work shift change effective date'] if item['Work shift change effective date'] else '',
        item['Location'] if item['Location'] else '',
        item['CF LRV location change effective date'] if item['CF LRV location change effective date'] else '',
        item['Country'] if item['Country'] else '',
        item['CF date of birth MM DD YYYY'] if item['CF date of birth MM DD YYYY'] else '',
        item['CF LRV manager email'] if item['CF LRV manager email'] else '',
        item['CF LRV manager first name'] if item['CF LRV manager first name'] else '',
        item['CF LRV manager last name'] if item['CF LRV manager last name'] else '',
        item['Legal entity'] if item['Legal entity'] else '',
        item['Worker sub type'] if item['Worker sub type'] else '',
        item['Cost center'] if item['Cost center'] else '',
        item['Workers CC change eff date'] if item['Workers CC change eff date'] else '',
        item['Years of service'] if item['Years of service'] else '',
        item['Pay group'] if item['Pay group'] else '',
        item['Japan special schedule flag'] if item['Japan special schedule flag'] else '',
        item['Continuous service date'] if item['Continuous service date'] else '',
        item['Time off service date'] if item['Time off service date'] else '',
    ]


def get_enabled_dept():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:effectively-enabled",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true"
                }
            }
        }
    }


MASTER_ROW_FIELDS = [
    'userid', 'workerreferenceemployeeid', 'emailaddress', 'firstname', 'lastname', 'workertype',
    'effective_date_of_worker_type', 'exemptionstatus', 'cf_lrv_job_exempt_eff_date', 'gender',
    'hiredate', 'terminationdate', 'active', 'function', 'function_change_effective_date',
    'businesstitle', 'cf_lrv_business_title_change', 'fieldhr', 'managerid',
    'effective_date_of_manager_change', 'work_shift', 'work_shift_change_effective_date',
    'location', 'location_change_eff_date', 'country', 'date_of_birth', 'cf_lrv_manager_email',
    'cf_lrv_manager_first_name', 'cf_lrv_manager_last_name', 'legalentity', 'worker_subType',
    'cost_center', 'worker_cc_change_date', 'year_of_service', 'paygroup',
    'continous_service_date', 'timeoff_service_date',
]


def process_each_user_payload(item):
    """Conf for one process_each_user run (master-side, one per valid row).

    Resolves existing-user state (useruri/status/startdate/enddate) from the bulk
    'userreferencereport' lookup held in load_replicon_userlist (France uses a report instead of
    per-user UserList searches), and resolves dept/legal/paygroup/cost uris from the master's
    pre-fetched lists. All resolved values arrive via conf so the per-user DAG needs no
    further Replicon lookups. supervisor_logger is the master's shared artifact so deferred
    supervisor entries land in one place for the master's fan-out."""
    payload = {key: item[key] for key in MASTER_ROW_FIELDS}

    user_id = (item.get('userid') or '').lower()
    users = rail.result('load_replicon_userlist') or []
    useruri = rail.find_first_by_attr_and_get_attr(users, 'userid', user_id, 'useruri', '') or ''
    enddate = rail.find_first_by_attr_and_get_attr(users, 'userid', user_id, 'enddate', '') or ''
    startdate = rail.find_first_by_attr_and_get_attr(users, 'userid', user_id, 'startdate', '') or ''
    report_status = rail.find_first_by_attr_and_get_attr(users, 'userid', user_id, 'status', '')
    status = ('true' if str(report_status).lower() == 'enabled' else 'false') if report_status else ''

    departmentgroupuri = ''
    if rail.result('get_department_list'):
        departmentgroupuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_department_list'), 'departmentgroupname', item['location'], 'departmentgroupuri', '')

    legalentityuri = ''
    if item['legalentity'] and rail.result('get_all_enabled_divisions'):
        legalentityuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_enabled_divisions'), 'displayText', item['legalentity'], 'uri', '')

    paygroupuri = ''
    if item['paygroup'] and rail.result('get_enabled_service_centers'):
        paygroupuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_enabled_service_centers'), 'displayText', item['paygroup'], 'uri', '')

    costcenteruri = ''
    if item['cost_center'] and rail.result('get_enabled_cost_centers'):
        costcenteruri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_enabled_cost_centers'), 'displayText', item['cost_center'], 'uri', '')

    payload.update({
        'useruri': useruri,
        'enddate': enddate,
        'startdate': startdate,
        'status': status,
        'departmentgroupuri': departmentgroupuri,
        'legalentityuri': legalentityuri,
        'paygroupuri': paygroupuri,
        'costcenteruri': costcenteruri,
        'supervisor_logger': rail.result('supervisor_logger_list'),
    })
    return payload


def _base_child_payload(dag_run):
    """The base conf every add/update/disable child receives, sourced from this per-user
    run's conf. 'logger' is THIS run's log artifact (gathered by the master after all
    per-user runs finish); 'supervisor_logger' is the master's shared artifact."""
    item = dag_run.conf
    return {
        'userid': item['userid'],
        'workerreferenceemployeeid': item['workerreferenceemployeeid'],
        'emailaddress': item['emailaddress'],
        'firstname': item['firstname'],
        'lastname': item['lastname'],
        'workertype': item['workertype'],
        'effective_date_of_worker_type': item['effective_date_of_worker_type'],
        'exemptionstatus': item['exemptionstatus'],
        'cf_lrv_job_exempt_eff_date': item['cf_lrv_job_exempt_eff_date'],
        'gender': item['gender'],
        'hiredate': item['hiredate'],
        'terminationdate': item['terminationdate'],
        'active': item['active'],
        'function': item['function'],
        'function_change_effective_date': item['function_change_effective_date'],
        'businesstitle': item['businesstitle'],
        'cf_lrv_business_title_change': item['cf_lrv_business_title_change'],
        'fieldhr': item['fieldhr'],
        'managerid': item['managerid'],
        'effective_date_of_manager_change': item['effective_date_of_manager_change'],
        'work_shift': item['work_shift'],
        'work_shift_change_effective_date': item['work_shift_change_effective_date'],
        'location': item['location'],
        'location_change_eff_date': item['location_change_eff_date'],
        'country': item['country'],
        'date_of_birth': item['date_of_birth'],
        'cf_lrv_manager_email': item['cf_lrv_manager_email'],
        'cf_lrv_manager_first_name': item['cf_lrv_manager_first_name'],
        'cf_lrv_manager_last_name': item['cf_lrv_manager_last_name'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger']
    }


def process_disable_user_payload(dag_run):
    """Disable child conf. When the user is already disabled in Replicon (status false)
    the termination date sent is the existing Replicon end date."""
    item = dag_run.conf
    uris = rail.result('get_all_req_uri_details_40')
    payload = _base_child_payload(dag_run)
    payload.update({
        'terminationdate': uris['enddate'] if uris['status'].lower() == 'false' else item['terminationdate'],
        'departmentgroupuri': uris['departmentgroupuri'],
        'useruri': uris['useruri'],
    })
    return payload


def process_update_user_payload(dag_run):
    item = dag_run.conf
    uris = rail.result('get_all_req_uri_details_40')
    payload = _base_child_payload(dag_run)
    payload.update({
        'departmentgroupuri': uris['departmentgroupuri'],
        'useruri': uris['useruri'],
        'rehireupdate': 'rehire' if uris['status'].lower() == 'false' else 'update',
        'legalentity': item['legalentity'],
        'worker_subType': item['worker_subType'],
        'cost_center': item['cost_center'],
        'worker_cc_change_date': item['worker_cc_change_date'],
        'year_of_service': item['year_of_service'],
        'paygroup': item['paygroup'],
        'continous_service_date': item['continous_service_date'],
        'timeoff_service_date': item['timeoff_service_date'],
        'legalentityuri': uris['legalentityuri'],
        'paygroupuri': uris['paygroupuri'],
        'costcenteruri': uris['costcenteruri'],
    })
    return payload


def process_add_user_payload(dag_run):
    item = dag_run.conf
    uris = rail.result('get_all_req_uri_details_40')
    payload = _base_child_payload(dag_run)
    payload.update({
        'effective_date_of_worker_type': item['effective_date_of_worker_type'] if item[
            'effective_date_of_worker_type'] else str(datetime.now().date()),
        'cf_lrv_business_title_change': item['cf_lrv_business_title_change'] if item[
            'cf_lrv_business_title_change'] else str(datetime.now().date()),
        'effective_date_of_manager_change': item['effective_date_of_manager_change'] if item[
            'effective_date_of_manager_change'] else str(datetime.now().date()),
        'location_change_eff_date': item['location_change_eff_date'] if item[
            'location_change_eff_date'] else str(datetime.now().date()),
        'departmentgroupuri': uris['departmentgroupuri'],
        'legalentity': item['legalentity'],
        'worker_subType': item['worker_subType'],
        'cost_center': item['cost_center'],
        'worker_cc_change_date': item['worker_cc_change_date'],
        'year_of_service': item['year_of_service'],
        'paygroup': item['paygroup'],
        'continous_service_date': item['continous_service_date'],
        'timeoff_service_date': item['timeoff_service_date'],
        'legalentityuri': uris['legalentityuri'],
        'paygroupuri': uris['paygroupuri'],
        'costcenteruri': uris['costcenteruri'],
    })
    return payload


def process_supervisor_mapper_data(item):
    """Conf for the shared supervisor-assignment child; item is a supervisor_logger_list
    entry ({'properties': {...}}). The deferred entries carry loginid/supervisorempid;
    the child reads managerid/loginname/userid, so the keys are mapped here.

    'logger' is the per-user run's log artifact stored in the deferred entry's properties
    (the child filters and rewrites that run's Add entry with the supervisor outcome)."""
    return {
        "managerid": item['properties']['supervisorempid'],
        "loginname": item['properties']['loginid'],
        "userid": item['properties']['loginid'],
        "useruri": item['properties']['useruri'],
        'type': item['properties']['type'],
        "sup_email": item['properties']['sup_email'],
        "sup_firstname": item['properties']['sup_firstname'],
        "sup_lastname": item['properties']['sup_lastname'],
        "sup_change_effective_date": item['properties']['sup_change_effective_date'],
        'logger': item['properties'].get('logger') or rail.result('logger_list'),
        'supervisor_logger': rail.result('supervisor_logger_list'),
    }


FAILURE_ACTION_BY_TASK = {
    'trigger_disable_user_with_enddate': 'Disable user',
    'wait_for_disable_user_with_enddate': 'Disable user',
    'trigger_disable_user': 'Disable user',
    'wait_for_disable_user': 'Disable user',
    'trigger_update_user_rehire': 'Update',
    'wait_for_update_user_rehire': 'Update',
    'trigger_update_user': 'Update',
    'wait_for_update_user': 'Update',
    'trigger_add_user': 'Add',
    'wait_for_add_user': 'Add',
}


def get_failure_log_entry(dag_run):
    """Error entry for a per-user run that failed anywhere (catch_and_log_errors).

    Names the failing task and derives the action from the branch that task belongs to."""
    context = rail.get_current_context()
    failed_task_ids = rail.lib.errors.get_failed_task_ids(context) or []
    error_message = rail.render_template('{{ get_error_message() }}')

    action = 'Add/Update'
    for task_id in failed_task_ids:
        if task_id in FAILURE_ACTION_BY_TASK:
            action = FAILURE_ACTION_BY_TASK[task_id]
            break

    if failed_task_ids:
        details = f"Failed at {', '.join(failed_task_ids)}: {error_message}"
    else:
        details = error_message

    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": action,
        "status": "Error",
        'details': details,
        'country': 'France'
    }


def log_process_user_payload(dag_run):
    """The Skipped log entry for the router's dead-end branches."""
    item = dag_run.conf
    uris = rail.result('get_all_req_uri_details_40')
    action = "Add"
    details = "User is  disabled in workday hence not added"
    if uris['useruri']:
        action = "Disable user"
        details = "User status (Active) received blank value or '-'"
        if str(uris['status']).lower() == 'false':
            if uris['enddate']:
                details = "User is already disabled in Replicon with end date"
            elif item['terminationdate']:
                if (datetime.strptime(item['terminationdate'], "%Y-%m-%d")).date() < datetime.now().date():
                    details = "User not disabled since end date received is in the past"
                elif uris['startdate'] and (datetime.strptime(item['terminationdate'], "%Y-%m-%d")).date() < datetime.strptime(
                        uris['startdate'], "%Y-%m-%d").date():
                    details = "User was already disabled in Replicon, end date was updated since end date received is in the past"
    return {
        "userid": item['userid'],
        "username": item['firstname'] + " " + item['lastname'],
        "action": action,
        "status": "Skipped",
        'details': details,
        'country': 'France'
    }


MANDATORY_FIELDS = {
    "userid": "User ID"
}


def get_mandatory_fields_exception_message(item):
    """Detail string: 'User ID must be present'."""
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} must be present")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_invalid_record(item):
    """Validation-skip row; stamps country 'France' on these entries."""
    details = get_mandatory_fields_exception_message(item)
    return {
        "userid": item['userid'],
        "username": item['firstname'] + " " + item['lastname'],
        "action": "Validation",
        "status": "Skipped",
        'details': details,
        'country': 'France'
    }
