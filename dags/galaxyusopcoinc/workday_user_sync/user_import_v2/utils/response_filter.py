import json
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload
DEFAULT_DELIMITER_FOR_FULL_PATH = "^"


def map_list_data(res):
    data = res.json()['d']['rows']
    return list(
        map(lambda item:
            {
                'name': item['cells'][0]['textValue'],
                'uri': item['cells'][0]['uri'],
                'code': item['cells'][1].get('textValue'),
            }, data)
    )


def map_supervisor_list(res):
    data = res.json()['d']['rows']
    return list(
        filter(lambda x: x['employeeid'] == request_payload.get_conf()['managerid'],
               map(lambda item:
                   {
                       'useruri': item['cells'][0]['uri'],
                       'employeeid': item['cells'][2].get('textValue'),
                       'enabled': item['cells'][1].get('boolValue'),
                   }, data))
    )


def map_response_data(res):
    data = res.json()['d']
    return list(
        map(lambda item:
            {
                'name': item['displayText'],
                'uri': item['uri'],
            }, data)
    )


def map_timeoff_data(dag_run):
    timeoff = dag_run.conf['timeofftypes']
    replicon_data = [{'timeoffname': timeoff_name}
                      for timeoff_name in dag_run.conf['time_off_types']]
    rail.set_result(key="timeoff_names", val=[
                    item['timeoffname'] for item in replicon_data])
    rail.set_result(key="toil_present", val=any("TOIL" in timeoff for timeoff in rail.result(
        "get_timeoff_toassign", "timeoff_names")))
    mapped_data = list(map(lambda item: rail.find_first_by_attr_and_get_attr(
        timeoff, 'name', item['timeoffname']), replicon_data))
    return mapped_data


def map_timesheetperiod_search_result(res):
    data = res.json()['d']
    timesheetperiodtype = request_payload.get_conf()['timesheetperiodtype']
    timesheet_period = list(
        filter(lambda x: x['displayText'] == timesheetperiodtype, data))
    if len(timesheet_period) == 0:
        rail.set_result(
            f'Timesheet period not updated since {timesheetperiodtype} is not available in Replicon', 'log')
        return None
    return timesheet_period[0]


def do_format_logs():
    def can_filter_record(log):
        if log['status'].lower() == "error" and log['action'].lower() == "add" and str(log['is_add_and_errored']).lower() == "true":
            return True
        if log['status'].lower() in ['success', 'exception'] and log['action'].lower() == "add":
            return True
        return False

    def get_record_summary(logs):
        rail.set_result(key="success", val=len(
            list(filter(lambda log: log['status'].lower() == 'success', logs))))
        rail.set_result(key="error", val=len(
            list(filter(lambda log: log['status'].lower() == 'error', logs))))
        rail.set_result(key="exception", val=len(
            list(filter(lambda log: log['status'].lower() == 'exception', logs))))
        return {
            "new_users_added": len(list(filter(can_filter_record, logs))),
            "users_updated": len(list(filter(lambda log: log['status'].lower() in ['success', 'exception', 'error']
                                             and log['action'].lower() == "update", logs)))
        }

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        return "Success"

    master_log = json.loads(rail.result('load_master_log'))

    gather_logs = rail.result('gather_logs') if rail.result('gather_logs') else []
    for log in gather_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)
    users = list(
        set(map(lambda x: x['properties'].get('employeeid', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for employeeid in users:
        user_logs = list(
            filter(lambda x: x['properties'].get('employeeid', '') == employeeid and x['properties'].get('message', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'employeeid': employeeid,
                'username': first['properties'].get('username'),
                'loginname': first['properties'].get('loginname'),
                'status': get_status(user_logs),
                'action': first['properties'].get('action'),
                'details': ";".join(list(set(map(lambda x: x['properties'].get('message'), user_logs)))),
                'jobid': first['ecid'],
                "is_add_and_errored": bool(list(filter(lambda x: x['properties'].get('is_add_and_errored') == "True", user_logs)))
            })

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }


def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)


def get_service_centers_date_handler(response):
    if not response['rows']:
        return []
    return list(map(lambda service_center: {
        "name": get_value(service_center, 0, "textValue"),
        "code": get_value(service_center, 1, "textValue"),
        "description": get_value(service_center, 2, "textValue"),
        "uri": get_value(service_center, 3, "uri")
    }, response['rows']))


def get_full_path(data, delimiter=DEFAULT_DELIMITER_FOR_FULL_PATH):
    return rail.smartjoin_by_delim(data, delimiter)


def get_all_division_from_replicon_filter(response):
    if not response['rows']:
        return []
    return list(map(lambda division: {
        "name": get_value(division, 0, 'textValue'),
        "uri":  get_value(division, 1, 'uri'),
        "full_path": get_full_path([item['textValue'] for item in get_value(division, 2, 'cellCollection')])
    }, response['rows']))


def get_all_employee_type_from_replicon_filter(response):
    if not response['rows']:
        return []
    return list(map(lambda emp_type: {
        "name": get_value(emp_type, 0, 'textValue'),
        "uri": get_value(emp_type, 1, 'uri'),
        "full_path": get_full_path([item['textValue'] for item in get_value(emp_type, 2, 'cellCollection')])
    }, response['rows']))


def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})


def get_effective_user_groupmembership_filter(response):
    group_list = ['costCenter', 'department', 'division',
                  'employeeType', 'location', 'serviceCenter']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))


def get_timeoff_type_fields(timeoff):
    return {
        "name": timeoff['timeOffType']['name'],
        "uri": timeoff['timeOffType']['uri'],
        "timeoff_allowed": timeoff['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": timeoff['policySetSchedule']
    }


def get_users_assigned_timeoff_filter(response):
    # add TOIL check here
    timeoff_list = rail.result("get_timeoff_toassign", "timeoff_names")
    rail.set_result(key='timeoff_to_remove_with_zero_line_policy',
                    val=list(filter(lambda to: bool(to['policy']) and to['name'] not in timeoff_list,
                                    map(get_timeoff_type_fields, response['policiesByTimeOffType']))))
    return timeoff_list
