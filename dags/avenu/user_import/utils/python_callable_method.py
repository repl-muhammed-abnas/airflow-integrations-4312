import ast
import json
import rail
from avenu.user_import.utils import request_payload
from airflow.models import Variable
from dateutil import relativedelta


def get_time_off_types_to_assign(dag_run, config):
    time_off_to_assign = []
    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))
    location = "All Other US Locations" if dag_run.conf['locationdescription'] in config.us_locations else (
        "CA Locations" if dag_run.conf['locationdescription'] in config.ca_locations else dag_run.conf['locationdescription'])
    data = list(filter(lambda x: x['employee_type'] == dag_run.conf['employee_type']
                       and x['location'] == location, user_sync_mapper))
    if data[0]['time_off_type'] == "NA":
        return time_off_to_assign
    for time_off_details in data[0]['time_off_type']:
        time_off_to_assign.append(time_off_details)
    return time_off_to_assign

# pylint: disable=too-many-return-statements
# pylint: disable=too-many-branches


def get_update_time_off_types_to_assign(dag_run, config):
    time_off_to_assign = []
    final_time_off_to_assign = []
    flag = False
    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))
    location = "All Other US Locations" if dag_run.conf['locationdescription'] in config.us_locations else (
        "CA Locations" if dag_run.conf['locationdescription'] in config.ca_locations else dag_run.conf['locationdescription'])

    # get timeoff type to assign based on employee type and derived location
    data = list(filter(lambda x: x['employee_type'] == dag_run.conf['employee_type']
                       and x['location'] == location, user_sync_mapper))
    if data[0]['time_off_type'] == 'NA':
        return ["NA"]
    for time_off_details in data[0]['time_off_type']:
        time_off_to_assign.append(time_off_details)
    existing_time_off_types_list = list(map(
        lambda x: x['name'], rail.result("get_user_time_off_policy_summary")))

    # Below is logic to check is the User Location is US location
    # then process for only Full Time
    # if it is canada then process for Full/Part Time
    timeoff_logic_flag = False
    if dag_run.conf['locationdescription'] in (config.us_locations + config.ca_locations):
        if dag_run.conf['workercategorydescription'] == "Full Time":
            timeoff_logic_flag = True
    else:
        if dag_run.conf['workercategorydescription'] in ["Full Time", "Part Time"]:
            timeoff_logic_flag = True

    for manual_timeoff in  config.timeoff_to_ignore:
        if manual_timeoff in existing_time_off_types_list:
            time_off_to_assign.append(manual_timeoff)
            final_time_off_to_assign.append(manual_timeoff)
    if timeoff_logic_flag:
        for time_off in existing_time_off_types_list:
            if time_off in config.special_time_off_policy:
                flag = True
                final_time_off_to_assign.append(time_off)
        if flag:
            for time_off in time_off_to_assign:
                if time_off in config.default_time_off_policy:
                    continue
                final_time_off_to_assign.append(time_off)
        else:
            if dag_run.conf["time_off_only"] == "Yes":
                return list(filter(lambda x: x != 'Holiday', time_off_to_assign))
            return time_off_to_assign
        if dag_run.conf["time_off_only"] == "Yes":
            return list(filter(lambda x: x != 'Holiday', final_time_off_to_assign))
        return final_time_off_to_assign
    if dag_run.conf["time_off_only"] == "Yes":
        return list(filter(lambda x: x != 'Holiday', time_off_to_assign))
    return time_off_to_assign


def get_update_time_off_types_to_delete(config):
    time_off_to_delete = []
    existing_time_off_types_list = list(map(
        lambda x: x['name'], rail.result("get_user_time_off_policy_summary")))
    time_off_to_assign = rail.result("time_off_types_to_assign")
    for time_off_disable in existing_time_off_types_list:
        if (time_off_disable not in time_off_to_assign) and (time_off_disable not in config.timeoff_to_ignore):
            time_off_to_delete.append(time_off_disable)
    return time_off_to_delete


def check_if_user_has_special_to(all_users_tos, config):
    if not all_users_tos:
        return False
    return bool(list(set(all_users_tos) & set(config.special_time_off_policy)))

def get_update_time_off_policy_to_assign(dag_run, config):
    user_has_special_to_assigned = check_if_user_has_special_to(rail.result('get_user_time_off_policy_summary', 'all_users_tos'), config)
    time_off_policy_to_assign = []
    final_time_off_policy_to_assign = []
    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))
    location = "All Other US Locations" if dag_run.conf['locationdescription'] in config.us_locations else (
        "CA Locations" if dag_run.conf['locationdescription'] in config.ca_locations else dag_run.conf['locationdescription'])
    data = list(filter(lambda x: x['employee_type'] == dag_run.conf['employee_type']
                       and x['location'] == location, user_sync_mapper))
    if data[0]['time_off_type'] == "NA":
        return []
    for time_off_details in data[0]['time_off_type']:
        time_off_policy_to_assign.append(time_off_details)
    existing_time_off_types_list = list(map(
        lambda x: x['name'], rail.result("get_user_time_off_policy_summary")))
    for time_off in time_off_policy_to_assign:
        if time_off in existing_time_off_types_list:
            continue
        if user_has_special_to_assigned and dag_run.conf['is_full_time_employee_type'].lower() == "yes" and time_off in config.default_time_off_policy:
            continue
        final_time_off_policy_to_assign.append(time_off)
    return final_time_off_policy_to_assign


def get_existing_time_off_type():
    return rail.result("get_user_time_off_policy_summary")


def get_timeoff_type_list():
    data = rail.result('time_off_types_to_assign')
    if data[0] == "NA":
        return [{'timeofftypename': 'NA', 'timeofftypeuri': 'NA'}]
    return list(map(lambda item: {
        'timeofftypename':  item,
        "timeofftypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                                                               'displayText', item, 'uri', 'Not Available')
    }, data))


def get_timeoff_policy_list():
    data = rail.result('get_time_off_policy_to_assign')
    if not data:
        return []
    return list(map(lambda item: {
        'timeofftypename':  item,
        "timeofftypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                                                               'displayText', item, 'uri', 'Not Available')
    }, data))


def get_time_off_type_uris():
    data = rail.result('get_timeoff_type_list')
    if data[0]['timeofftypename'] == "NA":
        return ["NA"]
    return list(map(lambda item: item['timeofftypeuri'], data))


def get_policy_to_assign():
    data = rail.result('get_default_time_off_policy_schedule')
    if not data:
        return None
    res = list(map(lambda item: {
        'description': 'effective',
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, data))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))


def get_user_logs_by_status(task_id):
    data = rail.load_all_records(rail.result(task_id))
    res = list(map(lambda x: x['message'], data))
    return res


def get_message_from_log():
    data = rail.load_all_records(rail.result('filter_master_logs'))
    return list(map(lambda item: {
        'message': item['message'],
        'severity': item['severity'],
        'status': item['properties']['status']
    }, data))


def get_all_policy_to_assign(dag_run):
    data = list(map(lambda x: {
        'effectiveDate': {
            'year': x['year'],
            'month': x['effectiveDate']['month'],
            'day': x['effectiveDate']['day']
        },
        'description': 'Effective on ' + request_payload.get_date_from_replicon_date(dag_run.conf['todays_date']).strftime("%m-%d-%Y"),
        'policySet': x['policy']
    }, rail.result("get_default_policy_set")))
    return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))


def get_default_policy_set():
    today = request_payload.get_today_date()
    data = rail.result(
        'get_default_timeoff_policy_set_schedule_for_timeofftype')
    return list(map(lambda item: {
        'effectiveDate': today,
        'year': today['year'] + item['startOffset']['offsetValue'],
        'policy': item['policySet']
    }, data))

def get_oldest_date_from_json_date_list(json_date_list):
    return sorted([request_payload.get_date_from_replicon_date(effective_date) for effective_date in json_date_list])[0]

def get_default_policy_set_rehire(dag_run):
    start_date = request_payload.get_date_from_replicon_date(request_payload.get_replicon_date(
        dag_run.conf['rehiredate'])) if dag_run.conf['rehiredate'] else request_payload.get_date_from_replicon_date(dag_run.conf['todays_date'])
    existing_policy_start_date = request_payload.get_date_from_replicon_date(request_payload.get_replicon_date(
        dag_run.conf['rehiredate'])) if dag_run.conf['rehiredate'] else request_payload.get_date_from_replicon_date(dag_run.conf['todays_date'])
    policy_to_assign = []
    policy_sets = []
    flag = False
    data = rail.result(
        'get_default_timeoff_policy_set_schedule_for_timeofftype_rehire')
    for time_off in data:
        if time_off['startOffset']['offsetValue'] == rail.result('get_tenure'):
            policy_sets.append({
                    'offset': time_off['startOffset']['offsetValue'],
                    'policy': time_off['policySet'],
                    'first': 'Yes'
                }
            )
            flag = True
        if time_off['startOffset']['offsetValue'] > rail.result('get_tenure'):
            policy_sets.append(
                {
                    'offset': time_off['startOffset']['offsetValue'],
                    'policy': time_off['policySet'],
                    'first': 'No'
                }
            )

    if not flag:
        temp = list(map(lambda time_off:{
                    'offset': time_off['startOffset']['offsetValue'],
                    'daydiff': rail.result('get_tenure')-time_off['startOffset']['offsetValue'],
                    'tobeconsidered': 'Yes' if time_off['startOffset']['offsetValue'] < rail.result('get_tenure') else 'No'}, data))

        # filter out only TO policies to be considered
        temp = list(filter(lambda temp_time_off: temp_time_off['tobeconsidered'] == "Yes", temp))

        # derive the lowers daydiff from temp list
        min_daydiff = sorted([temp_time_off['daydiff'] for temp_time_off in temp])[0] if [temp_time_off['daydiff'] for temp_time_off in temp] else 99999

        policy_sets += list(map(lambda final_time_off :{
                    'offset': final_time_off['offset'],
                    'policy': rail.find_first_by_attr_and_get_attr(
                data, 'startOffset.offsetValue', final_time_off['offset'], 'policySet'),
                    'first': 'Yes'},
                               filter(lambda temp_time_off: temp_time_off['tobeconsidered'] == "Yes" and temp_time_off['daydiff'] == min_daydiff, temp)))

    specific_time_off_policy = rail.result(
        'get_specific_user_time_off_policy_summary')

    for policy_line in specific_time_off_policy:
        if request_payload.get_date_from_replicon_date(policy_line['effectiveDate']) < existing_policy_start_date:
            policy_to_assign.append({
                'description': policy_line['description'],
                'effectiveDate': {
                    'day': policy_line['effectiveDate']['day'],
                    'month': policy_line['effectiveDate']['month'],
                    'year': policy_line['effectiveDate']['year']
                },
                'policySet': policy_line['policySet']})

    first_policy_start_date = get_oldest_date_from_json_date_list(list(map(
        lambda policy :policy['effectiveDate'],rail.result('get_specific_user_time_off_policy_summary'))))

    for policy in policy_sets:
        if policy['first'] == "Yes":
            policy_start_date = start_date if dag_run.conf['rehiredate'] else request_payload.get_date_from_replicon_date(
                dag_run.conf['todays_date'])
        else:
            policy_start_date = first_policy_start_date.replace(
                year=first_policy_start_date.year+policy['offset'])

        policy_to_assign.append(
            {
                'description': 'Effective on ' + policy_start_date.strftime("%m-%d-%Y"),
                'effectiveDate': {
                    'day': policy_start_date.day,
                    'month': policy_start_date.month,
                    'year': policy_start_date.year
                },
                'policySet': policy['policy']
            }
        )

    return json.dumps(ast.literal_eval(str(policy_to_assign).replace("'script'", "'scriptTarget'")))


def do_format_logs():

    user_import_log = json.loads(
        rail.result("load_master_log"))
    unique_users = list(
        set(map(lambda item: item['properties'].get(
            "employeeid", ''), user_import_log))
    )

    def get_log_details(user_logs):
        return ";".join(list(filter(bool, (set(map(lambda x: x['message'], user_logs))))))

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"
    logs = []
    # pylint: disable= cell-var-from-loop
    for employee_id in unique_users:
        user_logs = list(
            filter(lambda x: x['properties'].get(
                'employeeid', '') == employee_id, user_import_log)
        )
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append(
                {
                    "employeeid": employee_id,
                    "firstname": first['properties']['firstname'],
                    "lastname": first['properties']['lastname'],
                    "status": get_status(user_logs),
                    "ecid": first['ecid'],
                    "details": get_log_details(user_logs)
                }
            )
    return logs


def get_tenure(dag_run):
    if dag_run.conf['rehiredate']:
        return 0
    first_policy_effective_date = get_oldest_date_from_json_date_list(list(map(
        lambda policy :policy['effectiveDate'],rail.result('get_specific_user_time_off_policy_summary'))))
    today_date = request_payload.get_date_from_replicon_date(
        dag_run.conf['todays_date'])
    delta = relativedelta.relativedelta(today_date, first_policy_effective_date)
    return delta.years

def test_is_users_exempt_status_changed(dag_run):
    current_employee_type = rail.result("get_effective_group_membership_for_user")['employeeTypes']
    if current_employee_type:
        current_employee_type = current_employee_type[0]['employeeType']['employeeType']['displayText']
        feed_file_derived_employee_type = dag_run.conf['employee_type']
        return ("Non-exempt" in current_employee_type and "Non-exempt" not in feed_file_derived_employee_type) or ("Non-exempt" not in current_employee_type
                and "Non-exempt" in feed_file_derived_employee_type)
    return True

def check_if_user_location_is_changed(dag_run):
    current_location_assigned = rail.result("get_effective_group_membership_for_user")['locations']
    if (not current_location_assigned) and dag_run.conf['locationuri']:
        return True
    current_location_name = current_location_assigned[0].get("location",{}).get("location",{}).get("displayText", None)
    if (not current_location_name) and dag_run.conf['locationuri']:
        return True
    if dag_run.conf['locationuri'] and (dag_run.conf['locationdescription'] != current_location_name):
        return True
    return False

def check_if_user_location_is_updated(dag_run,config):
    current_location_assigned = rail.result("get_effective_group_membership_for_user")['locations']
    if (not current_location_assigned) and dag_run.conf['locationuri']:
        return True
    current_location_name = current_location_assigned[0].get("location",{}).get("location",{}).get("displayText", None)
    if (not current_location_name) and dag_run.conf['locationuri']:
        return True
    if dag_run.conf['locationuri'] and (dag_run.conf['locationdescription'] != current_location_name):
        if current_location_name not in config.ca_locations and dag_run.conf['locationdescription'] in config.ca_locations:
            return True
        if current_location_name in config.ca_locations and dag_run.conf['locationdescription'] not in config.ca_locations:
            return True
    return False

def is_both_exempt(dag_run):
    file_employee_type = dag_run.conf["employee_type"]
    user_employee_type = rail.result("get_effective_group_membership_for_user").get(
        "employeeTypes", [{}])[0].get("employeeType", {}).get("employeeType",{}).get("displayText", "")
    return not (("non-exempt" not in file_employee_type.lower()) and ("non-exempt" not in user_employee_type.lower()))


def get_timeoff_types_to_process_no_accrual(dag_run, timeoff_task_id, config):
    time_off_list = rail.result(timeoff_task_id)

    if dag_run.conf['positionstatus'].lower() == 'leave':
        def check_to_ignore_timeoff(item):
            return any((sub_string in item['timeOffType']['displayText'] for sub_string in config.timeoff_for_no_accrual))

        return list(filter(check_to_ignore_timeoff, time_off_list))
    return time_off_list

def get_timeoff_types_to_process_no_accrual_rehire(dag_run, timeoff_task_id, config):
    time_off_list = rail.result(timeoff_task_id)

    if dag_run.conf['positionstatus'].lower() == 'active' and (not dag_run.conf['user_profile_end_date']):
        def check_to_ignore_timeoff(item):
            return any((sub_string in item['name'] for sub_string in config.timeoff_for_no_accrual))

        return list(filter(check_to_ignore_timeoff, time_off_list))
    return time_off_list
