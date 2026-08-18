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

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_historical_policy_to_assign_list(dag_run, action, for_each_loop, config):
    data = rail.result(for_each_loop)['policy']
    if not data:
        return []
    def get_compare_date():
        if action =="update":
            return dag_run.conf['change_effective_date']
        if action =='rehire':
            if rail.result(for_each_loop)['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES and\
                (dag_run.conf['previous_employee_status'] =="Unpaid Leave" or dag_run.conf['previous_employee_status'] =="Paid Leave"):
                if dag_run.conf['assigned_event_reason_code']=='10' and \
                    list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
                    return dag_run.conf['change_effective_date']
            return dag_run.conf['start_date']
        return dag_run.conf['end_date'] if dag_run.conf['end_date'] else dag_run.conf['change_effective_date']

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(get_compare_date(), DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))

def get_no_accrual_policy_line(dag_run, action):
    if rail.result("for_each_time_off_type_no_accural")['timeoff_type_name'] == "[USA] Sick" and action =='disable':
        return rail.result("get_termination_policy_for_sick_timeoff_Type")

    effective_date = (datetime.strptime(dag_run.conf['end_date'],DATE_FORMAT)+timedelta(days=1)).strftime(DATE_FORMAT)\
        if action =='disable' and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']
    return [{
        "effectiveDate":get_replicon_date(effective_date),
        "description": "Added By Integration on"+
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

def get_required_time_off_type_details(required_timeoff_types_names,value,mannual_time_off_types=null):
    log_time_off_type_exception = []
    exception_message = ""
    data = rail.result('get_all_time_off_types')
    all_time_off_types_names = list(map(itemgetter('timeoff_type_name'), data))

    for item in required_timeoff_types_names:
        if item not in all_time_off_types_names:
            log_time_off_type_exception.append(item)

    if log_time_off_type_exception:
        exception_message = f"Time off Type - '{rail.smartjoin_by_delim(log_time_off_type_exception,',')}' not available in Replicon"

    if value =='update':
        for timeoff_type in mannual_time_off_types:
            if rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'),
                    'timeoff_type_name', timeoff_type, 'timeoff_type_name'):
                required_timeoff_types_names.append(timeoff_type)

    return {"time_off_type_exception_log": exception_message if log_time_off_type_exception else [],
            "result": list(filter(lambda time_off: time_off['timeoff_type_name'] in list(set(required_timeoff_types_names))
        ,data))}

def assigned_time_offs_types():
    data = rail.result('get_user_time_off_policy_summary')
    return list(filter(lambda x: x['enabled'], map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": item['enabled'],
        "policy": item['policy'] if item['policy'] else []
    }, data)))

def time_off_types_to_be_assigned(dag_run, reference_time_off_types, special_time_off_mapper, special_time_off_type_names):
    data = rail.result('get_required_time_off_type_details_to_assign')['result']
    compare_data = rail.result('assigned_time_offs_types')

    def get_policy(item):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', item['timeoff_type_uri'], 'policy')

    # pylint: disable=too-many-boolean-expressions
    # pylint: disable=too-many-return-statements
    # pylint: disable=too-many-branches
    def get_status(item):
        if item['timeoff_type_name'] == "[CAN] Jour personnel/Personal Days":
            if dag_run.conf['location_level_3'] != 'STCONSTANT':

                if dag_run.conf['pay_type'] in ['Salaried','Exception Hourly'] and dag_run.conf['previous_pay_type'] =="Hourly":
                    return 'No'

                if dag_run.conf['pay_type'] =="Hourly" and dag_run.conf['previous_pay_type'] in ['Salaried','Exception Hourly'] :
                    return 'No'

                if dag_run.conf['pay_type'] in ['Salaried','Exception Hourly'] and dag_run.conf['previous_pay_type'] in ['Salaried','Exception Hourly']:
                    if dag_run.conf['std_hrs'] and float(dag_run.conf['std_hrs'])!= (float(dag_run.conf['previous_std_hrs'])
                        if dag_run.conf['previous_std_hrs'] else null):
                        return "No"

                if dag_run.conf['assigned_location_grp']=="STCONSTANT":
                    return "No"

            if dag_run.conf['location_level_3'] == 'STCONSTANT':
                if dag_run.conf['assigned_location_grp']!="STCONSTANT":
                    return "No"



        if item['timeoff_type_name'] in reference_time_off_types and dag_run.conf['rehire']!='Yes':
            if item['timeoff_type_name'] == "[CAN] Vacances/Vacation":
                if dag_run.conf['job_code']!= dag_run.conf['current_assigned_job_code']:
                    if (str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("S1","S2","S3","S4","S5"))) or\
                    (str(dag_run.conf['job_code']).endswith(("A1","A2","T1","T2")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("A1","A2","T1","T2"))) or\
                    (str(dag_run.conf['job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")) \
                    and not str(dag_run.conf['current_assigned_job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))):
                        return 'No'

                if dag_run.conf['previous_full_part']=="Part-Time" and dag_run.conf['full_part'] =="Full-Time":
                    return 'No'

                if dag_run.conf['previous_full_part']=="Full-Time" and dag_run.conf['full_part'] =="Part-Time":
                    return 'No'

                if dag_run.conf['previous_full_part']=="Part-Time" and dag_run.conf['full_part'] =="Part-Time" \
                    and (float(dag_run.conf['std_hrs']) != float(dag_run.conf["previous_std_hrs"])):
                    return 'No'

            if item['timeoff_type_name'] == "[CAN] Anniversaire de service/ Service Anniversary":
                if dag_run.conf['job_code']!= dag_run.conf['current_assigned_job_code']:
                    if (str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("S1","S2","S3","S4","S5"))) or\
                    (str(dag_run.conf['job_code']).endswith(("A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))):
                        return 'No'

                if dag_run.conf["reg_temp"]=="Regular" and dag_run.conf["previous_reg_temp"]=="Temporary":
                    return "No"

                if float(dag_run.conf['std_hrs']) != float(dag_run.conf["previous_std_hrs"]):
                    return "No"

                if dag_run.conf['assigned_location_grp'] == 'STCONSTANT' and dag_run.conf['location_level_3'] != 'STCONSTANT':
                    return "No"

        if dag_run.conf['assigned_event_reason_code']=="10" and \
            list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],special_time_off_mapper)):
            if item['timeoff_type_name'] in special_time_off_type_names and  (dag_run.conf['previous_employee_status'] =="Unpaid Leave"
                or dag_run.conf['previous_employee_status'] =="Paid Leave"):
                return 'No'

        if item['timeoff_type_name'] =="[CAN] Journée Flexible/Flexible Day - St.Constant"\
            and dag_run.conf['rehire']!='Yes':
            if  dag_run.conf['location_level_3']=='STCONSTANT' and dag_run.conf['employee_type_grp']:
                if dag_run.conf['reg_temp'] not in dag_run.conf['employee_type_grp']:
                    return 'No'

        if rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'timeoff_type_name'):
            if item['timeoff_type_name']=="[CAN] Jour personnel (temporaires)/Personal Days Temp" and \
                    dag_run.conf['location_level_3'] != dag_run.conf['assigned_location_grp']:
                if dag_run.conf['location_level_3'] == 'STCONSTANT' or dag_run.conf['assigned_location_grp']=="STCONSTANT":
                    return 'No'
            return 'Yes'
        return 'No'

    return list(filter(lambda x: x['status']=='No',map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'enabled'),
        "status": get_status(item) if dag_run.conf['rehire']!='Yes' else 'No',
        "policy": get_policy(item)
    }, data)))

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

def is_vacances_time_off_and_job_code_changed(dag_run):
    if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] == "[CAN] Vacances/Vacation St. Constant":
        return False
    return bool(dag_run.conf['job_code']!= dag_run.conf['current_assigned_job_code'] and (str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")) and
        not str(dag_run.conf['current_assigned_job_code']).endswith(("S1","S2","S3","S4","S5"))) or\
        (str(dag_run.conf['job_code']).endswith(("A1","A2","T1","T2")) and
            not str(dag_run.conf['current_assigned_job_code']).endswith(("A1","A2","T1","T2"))) or\
        (str(dag_run.conf['job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")) \
        and not str(dag_run.conf['current_assigned_job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))))

def get_all_policy_to_assign(dag_run,config):
    if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] == "[CAN] Anniversaire de service/ Service Anniversary" \
        and dag_run.conf['rehire']=='Yes':
        data = rail.result('get_default_time_off_policy_schedule')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES \
        and not is_vacances_time_off_and_job_code_changed(dag_run):
        if dag_run.conf['previous_employee_status'] =="Unpaid Leave" or dag_run.conf['previous_employee_status'] =="Paid Leave":
            if dag_run.conf['assigned_event_reason_code']=='10' and \
                list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
                if datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT).strftime(DATE_FORMAT) < \
                    get_date_from_replicon_date(dag_run.conf['assigned_change_effective_date']).strftime(DATE_FORMAT):
                    data =rail.result('get_historical_policy_to_assign_list')
                    return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

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

def get_custom_policy_line(dag_run,config):

    def get_effective_date(date_str):
        mapper_value = list(filter(lambda x:x['event'] == dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))
        if mapper_value:
            date_obj = datetime.strptime(date_str, DATE_FORMAT)
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

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(get_compare_date(dag_run.conf['change_effective_date']), DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))


def time_off_type_to_be_updated(dag_run,config):
    data = dag_run.conf['time_off_types_to_assign']
    for time_off_type in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES:
        if time_off_type in data :
            return {
                "time_off_type_name": time_off_type,
                "time_off_type_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_time_off_policy_summary'), 'timeoff_type_name', time_off_type, 'timeoff_type_uri')
            } if rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_time_off_policy_summary'), 'timeoff_type_name', time_off_type, 'timeoff_type_uri') else null
    return null

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
