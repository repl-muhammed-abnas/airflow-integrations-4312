from datetime import datetime
from functools import lru_cache
import itertools
import json
from dateutil.parser import parse as date_parser
import rail
from airflow.exceptions import AirflowException

LOCATION_DELIMITER = " / "
PARENT_LEGAL_ENTITY = "Mammoet"
ACTIVITIES_TO_ASSIGN = [
        "Engineer","Project Manager","Controller",
     "Administrator","Safety Officer","Operator","Supervisor",
     "Yard","Truck Driver","Rigger","Welder","Mechanic"
    ]

DATE_FORMAT = "%Y-%m-%d"

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_compose_required_field(item, config):
    if not item:
        return []
    country_details = get_location_details_from_code(config.LOCATION_CODE_MAPPER_TO_USE, item['legal_entity_code'][:2])
    return {
        **item,
        **{
            'country' : country_details.get('Country'),
            'country_code' : country_details.get('Country_Code'),
            'country_iso_code' : country_details.get('ISO_Code'),
            "legal_entity_full_path": f"{PARENT_LEGAL_ENTITY}{LOCATION_DELIMITER}{item['legal_entity']}"
        }
    }


def is_both_date_are_same(date1:str, date2:str):
    return date_parser(date1) == date_parser(date2)

def get_today_date(return_as_date=None):
    now = datetime.utcnow()
    if return_as_date:
        return now.date()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_replicon_date_from_str(date_str, date_format=DATE_FORMAT, use_parser=False):
    _date = date_parser(date_str)
    if not use_parser:
        _date = datetime.strptime(date_str, date_format)
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_date_from_replicon_date(replicon_date):
    return datetime(replicon_date['year'], replicon_date['month'], replicon_date['day']).date()

def get_group_details(group, payload_value):
    # rail.result can be cached. Check performance with and without it
    return rail.find_first_by_attr_and_get_attr(rail.result(f"get_replicon_{group}_details"),
            "full_path", payload_value)

@lru_cache(maxsize=32)
def get_replicon_timeoffs():
    return rail.result("get_all_timeoffs")

@lru_cache(maxsize=32)
def get_replicon_policies():
    return rail.result('get_all_polices')

@lru_cache(maxsize=16)
def get_replicon_activities():
    return rail.result("get_all_activities")

@lru_cache(maxsize=16)
def get_replicon_holiday_calender(calender_name):
    return rail.result("get_all_holiday_calenders").get(calender_name, {})

@lru_cache(maxsize=16)
def get_all_payrule_scripts_form_replicon():
    return rail.result('get_all_payrule_scripts')

@lru_cache(maxsize=16)
def get_all_office_schedule_form_replicon():
    return rail.result('get_all_office_schedule')

def get_location_details_from_code(LOCATION_CODE_MAPPER_TO_USE, country_code):
    return rail.find_first_by_attr_and_get_attr(
        LOCATION_CODE_MAPPER_TO_USE, 'Country_Code', country_code, default={})

@lru_cache(maxsize=16)
def get_required_user_level_custom_fields():
    custom_fields = rail.result('get_all_user_custom_fields')
    return {
            "overtime_relance": rail.find_first_by_attr_and_get_attr(
                custom_fields, 'displayText', 'Overtime Relance'),
            "overtime_relance_effective_date": rail.find_first_by_attr_and_get_attr(
                custom_fields, 'displayText', 'Overtime Relance Effective Date'),
            "location": rail.find_first_by_attr_and_get_attr(
                custom_fields, 'displayText', 'User Country')
        }

def get_newly_added_cost_center_details():
    _cost_centers = rail.result("gather_cost_center_added_details")[0] if rail.result("gather_cost_center_added_details") else [] # the child will be called only once
    if not _cost_centers:
        return {}

    newly_added_cost_center_hash = dict()
    for newly_added_cost_center in _cost_centers['cost_center_list']:
        # adding cost center and cost center code to the hash map
        # added both as a key and value to make it easy to search
        # for both cost center and cost center code
        newly_added_cost_center_hash[newly_added_cost_center['cost_center']] = newly_added_cost_center['cost_center_code']
        newly_added_cost_center_hash[newly_added_cost_center['cost_center_code']] = newly_added_cost_center['cost_center']

    rail.set_result(key="newly_added_cost_center_details", val=newly_added_cost_center_hash)
    return newly_added_cost_center_hash

def get_office_schedule(office_schedule_name):
    return rail.find_first_by_attr_and_get_attr(get_all_office_schedule_form_replicon(), 'displayText' , office_schedule_name)

def get_payrule_scripts(payrule_name):
    return rail.find_first_by_attr_and_get_attr(get_all_payrule_scripts_form_replicon(), 'displayText' , payrule_name)

def get_activities_to_assign(cost_center, cost_center_code, ACTIVITY_MAPPER_TO_USE, config, multiple_user_record_processing=False, replicon_data=None):
    """
    Determines the activities to assign to a user based on the provided cost center and activity mapper.
    Args:
        cost_center (str): The cost center name associated with the user.
        cost_center_code (str): The cost center code associated with the user.
        ACTIVITY_MAPPER_TO_USE (list): A list of activity mappings containing cost center and activity code details.
        config: Configuration object containing instance information.
        multiple_user_record_processing (bool, optional): A flag indicating whether multiple user records are being processed. Defaults to False.
        replicon_data (dict, optional): A dictionary containing replicon data, used when `multiple_user_record_processing` is True. Defaults to None.
    Returns:
        tuple: A tuple containing:
            - activities_found_in_replicon (list): A list of activities found in replicon that match the activity codes in the mapper.
            - exception_message (str): An error/exception message if any issues are encountered, otherwise an empty string.
            - activities_not_found_in_replicon (list): A list of activity codes from the mapper that were not found in replicon.
            - mapper_activities (list): A list of activities from the mapper that match the provided cost center.
    Raises:
        AirflowException: If `ACTIVITY_MAPPER_TO_USE` is not defined.
    Notes:
        - If `cost_center` or `cost_center_code` is not provided (based on instance), the function returns an empty list.
        - If the cost center is newly added, the function returns an empty list and a message indicating that activity assignment is skipped.
        - If no activities are found in replicon, an error message is returned along with empty lists.
        - If no activities are found in the mapper for the given cost center, an error message is returned along with empty lists.
        - If one or more activities from the mapper are not found in replicon, an error message is returned along with the missing activities.
        - In production instance, uses cost center name for matching (existing stable behavior).
        - In non-production instances (trial, uat), uses cost center code for matching (new behavior per CR_2.6 for testing).
    """
    # Check if we're in production instance to determine matching strategy
    is_prod_instance = False #hasattr(config, 'instance') and config.instance == 'prod'
    
    if is_prod_instance:
        # Production: KEEP EXISTING behavior - use cost center name for stability
        if not cost_center:
            return [], "No cost center provided for CostCenter/Activity assignment", [], []
        search_value = cost_center
        search_field = 'cost_center_name'
        search_label = f"cost center `{cost_center}`"
    else:
        # Non-production (trial, uat): NEW behavior - use cost center code for testing (CR_2.6)
        if not cost_center_code:
            return [], "No cost center code provided for CostCenter/Activity assignment", [], []
        search_value = cost_center_code
        search_field = 'cost_center_code'
        search_label = f"cost center code `{cost_center_code}`"
    # added for logging purposes
    print(f"using search_field as `{search_field}` for getting activities")
    newly_added_cost_center_details = get_newly_added_cost_center_details()
    # Check if the cost center/code is newly added
    if newly_added_cost_center_details.get(search_value, False): # if cost center is newly added
        return [], f"{search_label} is newly added, Activity assignment is skipped", [], []

    replicon_activities = get_replicon_activities() if not multiple_user_record_processing else load_json_artifact(replicon_data['activities'])

    if not replicon_activities:
        return [], "No activities found in Replicon", [], []

    # Search activities from the mapper using appropriate field based on instance
    mapper_activities = list(filter(lambda activity: activity[search_field] == search_value, ACTIVITY_MAPPER_TO_USE))

    if not mapper_activities:
        return [], f"No activities found in the mapper for {search_label}", [], []

    activity_code_list = list(map(lambda _activity: _activity['activity_type_code'], mapper_activities))

    activities_found_in_replicon = list(filter(lambda activity: activity['code'] in activity_code_list, replicon_activities['activities']))
    activities_codes_found_in_replicon = list(map(lambda activity: activity['code'], activities_found_in_replicon))

    activities_not_found_in_replicon =list(filter(lambda activity_code: activity_code not in activities_codes_found_in_replicon, activity_code_list))

    if activities_not_found_in_replicon:
        return [], "One or more activities not found in Replicon", activities_not_found_in_replicon, mapper_activities

    return activities_found_in_replicon, "", activities_not_found_in_replicon, mapper_activities

def process_each_user_conf(dag_run, item, config):
    get_all_polices = get_replicon_policies()

    activities_to_assign, _message, activities_not_found_in_replicon, mapper_activities = get_activities_to_assign(
        item['cost_center'], 
        item.get('cost_center_code', ''), 
        config.ACTIVITY_MAPPER_TO_USE,
        config
    )

    get_all_permission_sets = rail.result("get_all_permission_sets")

    mapper_timeoffs = list(filter(lambda timeoff: timeoff['Country'] == item['country']
                                    and timeoff['Time Off Profile'] == item['timeoff_profile_code'], config.TIMEOFF_MAPPER_TO_USE))
    replicon_timeoffs = get_replicon_timeoffs()

    timesheet_template = rail.find_first_by_attr_and_get_attr(list(filter(lambda ts:ts['Country'] == item['country']
                                                ,config.TIMESHEET_MAPPER_TO_USE)), "Pay Rule Name", item['payrule_name'])
    if timesheet_template:
        timesheet_template['uri'] = rail.find_first_by_attr_and_get_attr(get_all_polices, 'name', timesheet_template['Timesheet Template'], 'uri')

    timezone_from_mapper = rail.find_first_by_attr_and_get_attr(
        config.TIMEZONE_MAPPER_TO_USE, "TimezoneID", item['time_zone']) if item['time_zone'] else None

    return{
        **{
            "payload_id": dag_run.conf['payload_id'],
            "supervisor_log": rail.result("create_supervisor_log"),
            "emp_record_index": "1"
        },
        **item,
        **{
            "groups": {
                "legal_entities": get_group_details('legal_entities', f"{PARENT_LEGAL_ENTITY}{LOCATION_DELIMITER}{item['legal_entity']}"),
                "employee_type": get_group_details('employee_type', item['employee_type_name']),
                "pay_grade": get_group_details('pay_grade', item['pay_grade_name']),
                "location": get_group_details('location', f"{item['country']}{LOCATION_DELIMITER}{item['location']}"),
                "cost_center": get_group_details('cost_center', item['cost_center'])
            },
            "user_templates": {
                "timeoff_template": rail.find_first_by_attr_and_get_attr(get_all_polices, 'name', 'Time Off'),
                "timesheet_template": timesheet_template
            },
            "user_permissions": {
                "supervisor": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Supervisor'),
                "basic": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Project Resource with Reports'),
            },
            "custom_fields": get_required_user_level_custom_fields(),
            "mapper_derived":{
                "timeoff_to_assign": list(map(lambda to: {**to, **{"uri": rail.find_first_by_attr_and_get_attr(
                    replicon_timeoffs, 'name', to['Time Off Type Name'], 'uri')}} , mapper_timeoffs)),
                "timesheet_template": timesheet_template,
                "timezone": timezone_from_mapper,
                "activities": {
                    "activities": activities_to_assign,
                    "_message": _message,
                    "activities_not_found_in_replicon": activities_not_found_in_replicon,
                    "mapper_activities": mapper_activities
                },
                "work_week": config.WORK_WEEK_MAPPER_TO_USE.get(item['country'], {}),
                "timesheet_period": config.TIMESHEET_PERIOD_MAPPER_TO_USE.get(item['country'], {})

            },
            "activities": {
                "activities": activities_to_assign if not _message else [],
                "exception": _message
            },
            "replicon_payrule_scripts": get_payrule_scripts(item['payrule_name']),
            "replicon_office_schedule": get_office_schedule(item['office_schedule_name']),
            "holiday_calender": get_replicon_holiday_calender(item['holiday_calendar_external_code'])
        }
    }

@lru_cache(maxsize=16)
def load_json_artifact(artifact_to_load):
    return rail.load_json_artifact(artifact_to_load)

@lru_cache(maxsize=16)
def get_required_user_level_custom_fields_for_multiple_user_record_processing():
    custom_fields = load_json_artifact(get_dag_run_conf()['replicon_data']['custom_fields'])
    return {
            "overtime_relance": rail.find_first_by_attr_and_get_attr(
                custom_fields, 'displayText', 'Overtime Relance'),
            "overtime_relance_effective_date": rail.find_first_by_attr_and_get_attr(
                custom_fields, 'displayText', 'Overtime Relance Effective Date'),
            "location": rail.find_first_by_attr_and_get_attr(
                custom_fields, 'displayText', 'User Country')
        }


def get_group_details_multiple_records_processing(artifact_to_load, input_payload_value):
    return rail.find_first_by_attr_and_get_attr(load_json_artifact(artifact_to_load),
            "full_path", input_payload_value)

def get_replicon_holiday_calender_multiple_records(calender_name, artifact_to_load):
    return load_json_artifact(artifact_to_load).get(calender_name, {})

def process_each_user_conf_for_multiple_user_records_processing(dag_run, item, config):
    replicon_data = dag_run.conf['replicon_data']

    get_all_polices = load_json_artifact(replicon_data['policies'])
    replicon_activities = load_json_artifact(replicon_data['activities'])
    get_all_permission_sets = load_json_artifact(replicon_data['permission_set'])
    replicon_timeoffs = load_json_artifact(replicon_data['timeoffs'])

    activities_to_assign, _message, activities_found_in_replicon, mapper_activities = get_activities_to_assign(
        item['cost_center'], 
        item.get('cost_center_code', ''), 
        config.ACTIVITY_MAPPER_TO_USE,
        config,
        True, 
        replicon_data
    )

    mapper_timeoffs = list(filter(lambda timeoff: timeoff['Country'] == item['country']
                                    and timeoff['Time Off Profile'] == item['timeoff_profile_code'], config.TIMEOFF_MAPPER_TO_USE))

    timesheet_template = rail.find_first_by_attr_and_get_attr(list(filter(lambda ts:ts['Country'] == item['country']
                                                ,config.TIMESHEET_MAPPER_TO_USE)), "Pay Rule Name", item['payrule_name'])
    if timesheet_template:
        timesheet_template['uri'] = rail.find_first_by_attr_and_get_attr(get_all_polices, 'name', timesheet_template['Timesheet Template'], 'uri')

    timezone_from_mapper = rail.find_first_by_attr_and_get_attr(
        config.TIMEZONE_MAPPER_TO_USE, "TimezoneID", item['time_zone']) if item['time_zone'] else None

    return{
        **{
            "payload_id": dag_run.conf['payload_id'],
            "supervisor_log": dag_run.conf["supervisor_log"],
            "emp_record_index" : dag_run.conf['emp_records_index']
        },
        **item,
        **{
            "groups": {
                "legal_entities": get_group_details_multiple_records_processing(
                    replicon_data['legal_entities'], f"{PARENT_LEGAL_ENTITY}{LOCATION_DELIMITER}{item['legal_entity']}"),
                "employee_type": get_group_details_multiple_records_processing(replicon_data['employee_type'], item['employee_type_name']),
                "pay_grade": get_group_details_multiple_records_processing(replicon_data['pay_grade'], item['pay_grade_name']),
                "location": get_group_details_multiple_records_processing(
                    replicon_data['location'], f"{item['country']}{LOCATION_DELIMITER}{item['location']}"),
                "cost_center": get_group_details_multiple_records_processing(replicon_data['cost_center'], item['cost_center'])
            },
            "user_templates": {
                "timeoff_template": rail.find_first_by_attr_and_get_attr(get_all_polices, 'name', 'Time Off'),
                "timesheet_template": timesheet_template
            },
            "user_permissions": {
                "supervisor": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Supervisor'),
                "basic": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Project Resource with Reports'),
            },
            "custom_fields": get_required_user_level_custom_fields_for_multiple_user_record_processing(),
            "mapper_derived":{
                "timeoff_to_assign": list(map(lambda to: {**to, **{"uri": rail.find_first_by_attr_and_get_attr(
                    replicon_timeoffs, 'name', to['Time Off Type Name'], 'uri')}} , mapper_timeoffs)),
                "timesheet_template": timesheet_template,
                "timezone": timezone_from_mapper,
                "activities": {
                    "activities": activities_to_assign,
                    "_message": _message,
                    "activities_not_found_in_replicon": activities_found_in_replicon,
                    "mapper_activities": mapper_activities
                },
                "work_week": config.WORK_WEEK_MAPPER_TO_USE.get(item['country'], {}),
                "timesheet_period": config.TIMESHEET_PERIOD_MAPPER_TO_USE.get(item['country'], {})

            },
            "activities": {
                "activities": activities_to_assign if not _message else [],
                "exception": _message
            },
            "replicon_payrule_scripts": rail.find_first_by_attr_and_get_attr(
                load_json_artifact(replicon_data['payrule']), 'displayText' , item['payrule_name']),
            "replicon_office_schedule": rail.find_first_by_attr_and_get_attr(
                    load_json_artifact(replicon_data['office_schedule']), 'displayText' , item['office_schedule_name']),
            "holiday_calender": get_replicon_holiday_calender_multiple_records(
                item['holiday_calendar_external_code'], replicon_data['holiday_calendar'])
        }
    }


def do_format_logs(dag_run):
    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        return "Success"

    master_log = rail.load_all_records(dag_run.conf['exception_log'])

    for log in dag_run.conf['logs']:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)
    users = list(
        set(map(lambda x: x['properties'].get('employee_id', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for employeeid in users:
        all_logs_for_empid = list(
            filter(lambda x: x['properties'].get('employee_id', '') == employeeid and x['properties'].get('details', ''), master_log))
        unique_emp_record_index = (set(map(lambda user: user['properties']['emp_record_index'], all_logs_for_empid)))
        for unique_idx in unique_emp_record_index:
            user_logs = list(filter(lambda user_log: user_log['properties']['emp_record_index']==unique_idx, all_logs_for_empid))
            if len(user_logs) > 0:
                first = user_logs[0]
                logs.append({
                    'payload_id': dag_run.conf['payload_id'],
                    'employee_id': employeeid,
                    'login_name': first['properties'].get('login_name'),
                    'status': get_status(user_logs),
                    'action': first['properties'].get('action'),
                    'details': ";".join(list(set(map(lambda x: x['properties'].get('details'), user_logs)))),
                    'jobid': first['ecid'],
                })
    rail.set_result(key="success", val=len(list(filter(lambda log: log['status'].lower() == 'success', logs))))
    rail.set_result(key="error", val=len(list(filter(lambda log: log['status'].lower() == 'error', logs))))
    rail.set_result(key="exception", val=len(list(filter(lambda log: log['status'].lower() == 'error', logs))))
    return json.dumps(logs, ensure_ascii=False)


def get_timeoff_assignment_log_message(dag_run):
    log = []
    timeoff_not_found = list(map(lambda to: to['Time Off Type Name'],
                            filter(lambda timeoff: timeoff['uri'] is None, dag_run.conf['mapper_derived']['timeoff_to_assign'])))
    if timeoff_not_found:
        log.append(f"{rail.smartjoin_by_delim(timeoff_not_found, ';')} timeoffs not assigned to the user")

    timeoff_found = list(map(lambda to: to['Time Off Type Name'],
                            filter(lambda timeoff: timeoff['uri'], dag_run.conf['mapper_derived']['timeoff_to_assign'])))
    if timeoff_found:
        log.append(f"{rail.smartjoin_by_delim(timeoff_found, ';')} timeoffs assigned to the user")

    return rail.smartjoin_by_delim(log, ';')


def get_all_triggered_child_for_task_id(config,task_id):
    return list(itertools.chain(
        *list(filter(None, map(lambda x: rail.result(
                    f'{task_id}_{x+1}'), range(config.trigger_parallel_dagrun_count_process_users))))))
