from datetime import datetime, timedelta
import itertools
import json
import ast
from operator import itemgetter
import rail

DATE_FORMAT = "%m/%d/%Y"
null = None

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_replicon_date(date_str):
    if not date_str:
        return None

    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


def get_all_distinct_activity_names():
    activity_details_from_payload = rail.load_all_records(rail.result('query_distinct_activities_in_payload'))
    all_activities_replicon = rail.result('get_all_activity')
    activity_name_list = []
    for activity in activity_details_from_payload:
        if len(activity.split(" ")) <=1:
            activity_name_list.append(activity)
        else:
            for i in activity.split(" "):
                activity_name_list.append(i)

    unique_activity_names = set(activity_name_list)

    activities_to_create =[]
    for activity_name in unique_activity_names:
        if rail.find_first_by_attr_and_get_attr(all_activities_replicon,'displayText',activity_name,'displayText'):
            activities_to_create.append(activity_name)

    return {
        'distinct_activity_names': unique_activity_names,
        'activities_to_create': activities_to_create
    }


# def get_placeholder_time_off_to_be_assigned(dag_run, timeoff_type_mapper, timeoff_type):

#     if timeoff_type =="[UK] Sick leave":
#         key_mapping_for_feed_felids = {'company_code': 'company_code'}
#         mapper_keys_for_data_retrieve = ['company_code']

#     if timeoff_type =="[UK] Annual leave":
#         key_mapping_for_feed_felids = {'company_code': 'company_code'}
#         mapper_keys_for_data_retrieve = ['company_code']

#     def is_timeoff_type_mapper_value_found(mapper_data,value_found=False):

#         def bool_value_check(key,input_value,mapper_value, value_type):
#             print(input_value,mapper_value,"$$$$$$$$$$$$$$$$")
#             if value_type=="list":
#                 return bool(input_value in mapper_value)
#             return bool(input_value == mapper_value)

#         for key in mapper_keys_for_data_retrieve:
#             print(key,"################")
#             if isinstance(mapper_data[key], list):
#                 value_found = bool_value_check(key, dag_run.conf[key_mapping_for_feed_felids.get(key)], mapper_data[key], "list")
#             else:
#                 value_found = bool_value_check(key, dag_run.conf[key_mapping_for_feed_felids.get(key)], mapper_data[key], "str")

#             if not value_found:
#                 break

#         return value_found

#     for mapper_row in timeoff_type_mapper:
#         if is_timeoff_type_mapper_value_found(mapper_row):
#             return mapper_row['placeholder_policy']

#     return null

def get_time_off_to_be_assigned(dag_run, config):
    timeoff_types_to_assign = []

    def append_timeoff_types_to_assign(actual_name, placeholder_name, enabled=True):
        return timeoff_types_to_assign.append({
                    "actual_timeoff_type_name": actual_name,
                    "placeholder_timeoff_type_name": placeholder_name,
                    "enabled": enabled
                })

    employee_type_timeoff_mapper = {
        "[BRA] Interns": config.BRA_INTERNS_TIMEOFF_TYPES,
        "[BRA] Regulars": config.BRA_Regulars_TIMEOFF_TYPES,
        "[BRA] Regulars OT Eligible": config.BRA_Regulars_OT_Eligible_TIMEOFF_TYPES,
        "[BRA] Young Apprentice": config.BRA_Young_Apprentice_TIMEOFF_TYPES,
    }

    employee_type_name = dag_run.conf['employee_type_name']

    # Time off types that must end up disabled after assignment for this employee type (Req 5).
    disabled_timeoff_types = config.DISABLED_AFTER_ASSIGNMENT_TIMEOFF_TYPES.get(employee_type_name, [])

    applicable_timeoff_types = employee_type_timeoff_mapper.get(employee_type_name, []) \
        + config.All_BRA_Employee_Types_TIMEOFF_TYPES

    already_added = set()
    for timeoff_type in applicable_timeoff_types:
        if timeoff_type not in already_added:
            already_added.add(timeoff_type)
            append_timeoff_types_to_assign(timeoff_type, "NA", timeoff_type not in disabled_timeoff_types)

    return timeoff_types_to_assign

def get_required_time_off_type_details(required_timeoff_types_details,action, mannual_time_off_types=null):
    log_time_off_type_exception = []
    exception_message = ""
    data = rail.result('get_all_time_off_types')
    all_time_off_types_names = list(map(itemgetter('timeoff_type_name'), data))
    timeoff_type_names_to_be_assigned = list(map(itemgetter('actual_timeoff_type_name'), required_timeoff_types_details))

    for item in timeoff_type_names_to_be_assigned:
        if item not in all_time_off_types_names:
            log_time_off_type_exception.append(item)

    if log_time_off_type_exception:
        exception_message = f"Time off Type - '{rail.smartjoin_by_delim(log_time_off_type_exception,',')}' not available in Replicon"

    if action =='update':
        for timeoff_type in mannual_time_off_types:
            if rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'),
                    'timeoff_type_name', timeoff_type, 'timeoff_type_name'):
                timeoff_type_names_to_be_assigned.append(timeoff_type)

    return {"time_off_type_exception_log": exception_message if log_time_off_type_exception else [],
            "result": list(filter(lambda time_off: time_off['timeoff_type_name'] in timeoff_type_names_to_be_assigned,data))}

def assigned_time_offs_types():
    data = rail.result('get_user_time_off_policy_summary')
    return list(filter(lambda x: x['enabled'], map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": item['enabled'],
        "policy": item['policy'] if item['policy'] else []
    }, data)))

def time_off_types_to_be_disabled():
    def get_policy(item):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', item['timeoff_type_uri'], 'policy')

    compare_data = rail.result('get_required_time_off_type_details_to_assign')['result']
    data = rail.result('assigned_time_offs_types')
    return list(filter(lambda x: x['status'] == 'No', map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": item['enabled'],
        "status": 'Yes' if rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'timeoff_type_name') else 'No',
        "policy": get_policy(item)
    }, data)))

def get_historical_policy_to_assign_list(dag_run, action, for_each_loop):
    data = rail.result(for_each_loop)['policy']
    if not data:
        return []
    def get_compare_date():
        if action =="update":
            return dag_run.conf['change_effective_date']
        if action =='rehire':
            return dag_run.conf['start_date']
        return dag_run.conf['end_date'] if dag_run.conf['end_date'] else dag_run.conf['change_effective_date']

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(get_compare_date(), DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))

def get_no_accrual_policy_line(dag_run, action):
    effective_date = (datetime.strptime(dag_run.conf['end_date'],DATE_FORMAT)+timedelta(days=1)).strftime(DATE_FORMAT)\
        if action =='disable' and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']
    return [{
        "effectiveDate":get_replicon_date(effective_date),
        "description": "Effective on"+
            f"{dag_run.conf['end_date'] if action =='disable'and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']}",
        "policySet": {
            "timeOffBalanceEventScripts":[{
             "script": {
                "description": "Set initial balance for the first day of a policy",
                "name": "Starting Balance Set To",
                "uri": dag_run.conf['starting_balance_script_uri']
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                "number": rail.result('get_balance_summary_for_user')['timeRemaining']
                }
            }]
            }],
            "timeOffValidationScripts": [{
            "script": {
                "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                "name": "Prevent balance overdraw",
                "uri": dag_run.conf['prevent_balance_overdraw_uri']
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                "value": {
                "number": "0"
                }
            }],
            }]
        }
        }]

def get_all_policy_to_assign_for_disable_user():
    if rail.result('for_each_time_off_type_no_accural')['policy'] and rail.result('get_no_accrual_policy_line'):
        data =rail.result('get_historical_policy_to_assign_list_disable_user') + rail.result('get_no_accrual_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('for_each_time_off_type_no_accural')['policy'] and rail.result('get_no_accrual_policy_line'):
        data = rail.result('get_no_accrual_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_no_accural')['policy'] and not rail.result('get_no_accrual_policy_line'):
        data =rail.result('get_historical_policy_to_assign_list_disable_user')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    return null

def time_off_types_to_be_assigned_update(dag_run, config):
    data = rail.result('get_required_time_off_type_details_to_assign')['result']
    compare_data = rail.result('assigned_time_offs_types')

    def get_policy(item):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', item['timeoff_type_uri'], 'policy')

    def get_status(item):
        # pylint: disable=too-many-return-statements

        if rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'timeoff_type_name'):
            return 'Yes'

        return 'No'

    return list(filter(lambda x: x['status']=='No',map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'enabled'),
        "status": get_status(item) if dag_run.conf['rehire']!='Yes' else 'No',
        "policy": get_policy(item)
    }, data)))

def create_pay_grps_add_payload():
    pay_grps_to_add = rail.load_all_records(rail.result('query_pay_grp_udf_values_add'))
    current_drop_down_details = rail.result("get_pay_grp_dropdown_values")

    data = current_drop_down_details + pay_grps_to_add

    def get_payload(item):
        return {
            "target": {
                "uri": item['uri'],
                "name": null
            } if item.get('uri') else null,
            "name": item['name'] if item.get('name') else item['pay_grp'],
            "isEnabled": item.get('enabled', 1)
        }

    return list(map(get_payload, data))

# def get_placeholder_policy_annual(dag_run,config):
#     placeholder = get_placeholder_time_off_to_be_assigned(dag_run, config.ANNUAL_TO_PLACEHOLDER,
#         rail.result("for_each_time_off_assign_default_policy")['timeoff_type_name'])
#     if placeholder:
#         return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
#             'timeoff_type_name',placeholder,"timeoff_type_uri")
#     return None

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="total_record_count",val= dag_run.conf['total_records'])

    return final_log_records

def get_process_users_dag_ids(parallel_count):
    active_users =  list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_active_users_{x+1}') if rail.result(
            f'process_active_users_{x+1}') else []), range(parallel_count)))))

    disable_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_disable_users_{x+1}') if rail.result(
            f'process_disable_users_{x+1}') else []), range(parallel_count)))))

    return active_users + disable_users

def time_off_type_to_be_updated(dag_run, config):
    return list(filter(lambda x:x["time_off_type_name"] in config.SPECIAL_ACCRUAL_TO_TYPES and bool(x['time_off_type_uri']) ,map(
        lambda item:{
            "time_off_type_name": item["actual_timeoff_type_name"],
            "time_off_type_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_time_off_policy_summary'), 'timeoff_type_name', item["actual_timeoff_type_name"], 'timeoff_type_uri')
        },dag_run.conf['time_off_types_to_assign'])))

def get_historical_policy_to_assign_special_accrual_list(dag_run,time_off_type_uri,config):
    data = rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', time_off_type_uri, 'policy')
    if not data:
        return []
    def get_compare_date(date_str):
        mapper_value = list(filter(lambda x:x['event'] == dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))
        if mapper_value:
            date_obj = datetime.strptime(date_str, DATE_FORMAT)
            if mapper_value[0]['count']:
                new_date= date_obj+timedelta(weeks=int(mapper_value[0]['count']))
                return new_date.strftime(DATE_FORMAT)
        return date_str

    def get_effective_date_for_comparision():
        if dag_run.conf['previous_emp_status'] in ['Unpaid Leave', 'Paid Leave'] and \
            list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
            if dag_run.conf['assigned_event'] == "PAIEWP":
                return get_date_from_replicon_date(dag_run.conf['assigned_change_effective_date']).date()+timedelta(days=1)
            if list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))[0]['fmla']=="Yes" and \
                    list(filter(lambda x: x['event']==dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))[0]['fmla']=="No":
                return get_date_from_replicon_date(dag_run.conf['assigned_change_effective_date']).date()+timedelta(days=1)
        return datetime.strptime(get_compare_date(dag_run.conf['change_effective_date']), DATE_FORMAT).date()

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < get_effective_date_for_comparision(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))

def get_custom_policy_line(dag_run,config):

    def get_effective_date(date_str):
        mapper_value = list(filter(lambda x:x['event'] == dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))
        if mapper_value:
            date_obj = datetime.strptime(date_str, DATE_FORMAT)
            if dag_run.conf['assigned_event']=="PAIEWP" and \
                list(filter(lambda x: x['event']==dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))[0]['fmla']=="Yes":
                date_obj = get_date_from_replicon_date(dag_run.conf['assigned_change_effective_date'])
            if mapper_value[0]['count']:
                new_date= date_obj+timedelta(weeks=int(mapper_value[0]['count']))
                return new_date.strftime(DATE_FORMAT)
        return date_str

    return [{
        "effectiveDate":get_replicon_date(get_effective_date(dag_run.conf['change_effective_date'])),
        "description": f"Added By Integration on { dag_run.conf['change_effective_date']}",
        "policySet": []
        }]

def get_all_policy_to_assign_for_special_accrual():
    if rail.result('get_historical_policy_to_assign_special_accrual') and rail.result('get_custom_policy_line'):
        data =rail.result('get_historical_policy_to_assign_special_accrual') + rail.result('get_custom_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('get_historical_policy_to_assign_special_accrual') and rail.result('get_custom_policy_line'):
        data = rail.result('get_custom_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('get_historical_policy_to_assign_special_accrual') and not rail.result('get_custom_policy_line'):
        data =rail.result('get_historical_policy_to_assign_special_accrual')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null

def get_all_policy_to_assign_update():
    if rail.result('for_each_time_off_type_policy')['policy'] and rail.result('get_default_time_off_policy_schedule'):
        data =rail.result('get_historical_policy_to_assign_list') + rail.result('get_default_time_off_policy_schedule')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('for_each_time_off_type_policy')['policy'] and rail.result('get_default_time_off_policy_schedule'):
        data = rail.result('get_default_time_off_policy_schedule')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_policy')['policy'] and not rail.result('get_default_time_off_policy_schedule'):
        data =rail.result('get_historical_policy_to_assign_list')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null


def check_special_timeoff_update_required(dag_run, special_timeoff_mapper):
    if dag_run.conf['previous_emp_status'] =="Active" and dag_run.conf['emp_status'] in ['Unpaid Leave', 'Paid Leave']:
        return True
    if dag_run.conf['event']== dag_run.conf['assigned_event']:
        return False
    if dag_run.conf['previous_emp_status'] in ['Unpaid Leave', 'Paid Leave']  and dag_run.conf['emp_status'] in ['Unpaid Leave', 'Paid Leave']:
        if dag_run.conf['event']!=dag_run.conf['assigned_event']:
            if list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],special_timeoff_mapper))[0]['fmla']=="No" and \
                list(filter(lambda x: x['event']==dag_run.conf['event'],special_timeoff_mapper))[0]['fmla']=="No":
                return False
            if dag_run.conf['assigned_event'] =="PAIEWP" and \
                list(filter(lambda x: x['event']==dag_run.conf['event'],special_timeoff_mapper))[0]['fmla']=="Yes":
                return True
            if list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],special_timeoff_mapper))[0]['fmla']=="Yes" and \
                list(filter(lambda x: x['event']==dag_run.conf['event'],special_timeoff_mapper))[0]['fmla']=="Yes":
                return False
            if list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],special_timeoff_mapper))[0]['fmla']=="No" and \
                list(filter(lambda x: x['event']==dag_run.conf['event'],special_timeoff_mapper))[0]['fmla']=="Yes":
                return False
            if list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],special_timeoff_mapper))[0]['fmla']=="Yes" and \
                list(filter(lambda x: x['event']==dag_run.conf['event'],special_timeoff_mapper))[0]['fmla']=="No":
                return True
    return False
