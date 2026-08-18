from datetime import datetime
import json
import ast
import rail

DATE_FORMAT = "%d/%m/%Y"
null =None

def create_hr_manager_udf_add_payload():
    hr_manager_to_add = rail.load_all_records(rail.result('query_hr_manager_udf_values_add'))
    current_drop_down_details = rail.result("get_hr_manager_udf_dropdown_values")

    data = current_drop_down_details + hr_manager_to_add

    def get_payload(item):
        return {
            "target": {
                "uri": item['uri'],
                "name": null
            } if item.get('uri') else null,
            "name": item['name'] if item.get('name') else item['hr_manager_id'],
            "isEnabled": item.get('enabled', 1)
        }

    return list(map(get_payload, data))

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
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))

    return  final_log_records

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_historical_policy_to_assign_list(dag_run):
    data = rail.result('for_each_time_off_type_policy')['policy']
    if not data:
        return []

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))

def time_off_types_to_be_assigned():
    data = rail.result('get_enabled_time_off_types')

    def get_policy(item):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', item['timeoff_type_uri'], 'policy')

    return list(map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "policy": get_policy(item) if get_policy(item) else []
    }, data))

def get_all_policy_to_assign_compensation_day_rehire():
    if rail.result('for_each_time_off_type_policy')['policy'] and rail.result('get_custom_time_off_policy_schedule_compensation_day_rehire'):
        data =rail.result('get_historical_policy_compensation_day_rehire') + rail.result('get_custom_time_off_policy_schedule_compensation_day_rehire')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('for_each_time_off_type_policy')['policy'] and rail.result('get_custom_time_off_policy_schedule_compensation_day_rehire'):
        data = rail.result('get_custom_time_off_policy_schedule_compensation_day_rehire')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_policy')['policy'] and not rail.result('get_custom_time_off_policy_schedule_compensation_day_rehire'):
        data =rail.result('get_historical_policy_compensation_day_rehire')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null

def assigned_time_offs_types():
    data = rail.result('get_user_time_off_policy_summary')
    return list(filter(lambda x: x['enabled'], map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": item['enabled'],
        "policy": item['policy'] if item['policy'] else []
    }, data)))

def get_historical_policy_compensation_day_update(dag_run):
    def get_compensation_day_uri():
        return rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'),
                'timeoff_type_name',"Compensation Day", 'timeoff_type_uri')

    data = rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'),
            'timeoff_type_uri',get_compensation_day_uri(), 'policy') if get_compensation_day_uri() else []

    if not data:
        return []

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))


def get_all_policy_to_assign_compensation_day_update():
    if rail.result('get_historical_policy_compensation_day_update') and rail.result('get_time_off_policy_schedule_compensation_day_update'):
        data =rail.result('get_historical_policy_compensation_day_update') + rail.result('get_time_off_policy_schedule_compensation_day_update')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('get_historical_policy_compensation_day_update') and rail.result('get_time_off_policy_schedule_compensation_day_update'):
        data = rail.result('get_time_off_policy_schedule_compensation_day_update')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('get_historical_policy_compensation_day_update') and not rail.result('get_time_off_policy_schedule_compensation_day_update'):
        data =rail.result('get_historical_policy_compensation_day_update')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null
