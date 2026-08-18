# pylint: disable=unused-variable,too-many-statements,too-many-branches
from datetime import datetime
import json
import rail

DEFAULT_DELIMITER_FOR_FULL_PATH = "^"

def do_format_logs():

    def get_filtered_records(logs, status):
        return list(filter(lambda log: log['status'].lower() == status, logs))

    def get_record_summary(logs):
        return {
            "success": len(get_filtered_records(logs, 'success')),
            "failed":  len(get_filtered_records(logs, 'error')),
            "skipped":  len(get_filtered_records(logs, 'skipped')),
            "exception": len(get_filtered_records(logs, "exception"))
        }

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

    master_log = json.loads(rail.result('load_master_log'))

    gather_logs = rail.result('gather_user_logs') if rail.result('gather_user_logs') else []

    for log in gather_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)

    users = list(
        set(map(lambda x: x['properties'].get('employeeid', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for user in users:
        if not user:
            continue
        user_logs = list(
            filter(lambda x: x['properties'].get('employeeid', '') == user and x['properties'].get('details', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'employeeid': user,
                'action': first['properties'].get('action'),
                'status': get_status(user_logs),
                'details': ",".join(list(map(lambda x: x['properties'].get('details'), user_logs))),
                "ecid": first['ecid']
            })

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }

def get_missing_fields(dag_run):
    missing_field_list = []
    if not dag_run.conf['employee_type_uri']:
        missing_field_list.append('Employee Type not available')
    if not dag_run.conf['location_uri']:
        missing_field_list.append('Location not available')
    if not dag_run.conf['department_uri']:
        missing_field_list.append('Department not available')
    if not dag_run.conf['cost_center_uri']:
        missing_field_list.append('Cost Center not available')
    return {
        "value":','.join(missing_field_list)
    }

def get_status_and_details_for_update(dag_run):
    message = "Success"
    details = "User updated successfully"
    has_exception_message = get_missing_fields(dag_run)['value']
    if has_exception_message:
        message = "Exception"
        details = "User updated partially" + has_exception_message
    return {
        "employeeid": dag_run.conf['employeeid'],
        "action": "Update",
        "status": message,
        'details': details
    }

def validate_date_fields(dag_run):
    try:
        if dag_run.conf['startdate']:
            datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y")
        if dag_run.conf['enddate']:
            datetime.strptime(dag_run.conf['enddate'], "%m/%d/%Y")
        if dag_run.conf['firstdayofleave']:
            datetime.strptime(dag_run.conf['firstdayofleave'], "%m/%d/%Y")
        if dag_run.conf['returndatefromleave']:
            datetime.strptime(dag_run.conf['returndatefromleave'], "%m/%d/%Y")
        return True
    except Exception:
        return False

def get_effectivegroup_membership_filter(response):
    if not response:
        return []
    effective_groups = {}
    if response['costCenters']:
        effective_groups["costcenter"] = {
            "name": response['costCenters'][0]['costCenter']['costCenter']['displayText'],
            "uri": response['costCenters'][0]['costCenter']['costCenter']['uri']
        }

    if response['departments']:
        effective_groups["departmentname"] = {
            "name": response['departments'][0]['department']['department']['displayText'],
            "uri": response['departments'][0]['department']['department']['uri']
        }

    if response['employeeTypes']:
        effective_groups['employeetype'] = {
            "name": response['employeeTypes'][0]['employeeType']['employeeType']['displayText'],
            "uri": response['employeeTypes'][0]['employeeType']['employeeType']['uri']
        }

    if response['locations'] and response['locations'][0]['location']:
        effective_groups['location'] = {
            "name": response['locations'][0]['location']['location']['displayText'],
            "uri": response['locations'][0]['location']['location']['uri']
        }

    return effective_groups

def get_full_path(data, delimiter=DEFAULT_DELIMITER_FOR_FULL_PATH):
    return rail.smartjoin_by_delim(data, delimiter)

def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def get_all_group_data_from_replicon_filter(response):
    data = response['rows']
    if not data:
        return []
    return list(map(lambda item: {
        "name": get_value(item, 0, 'textValue'),
        "uri": get_value(item, 1, 'uri'),
        "full_path": get_full_path([item['textValue'] for item in get_value(item, 2, 'cellCollection')])
    }, data))

def get_data_from_replicon(response):
    if not response:
        return []
    return list(map(lambda x: {
        'name' : x['displayText'],
        'uri': x['uri']
    }, response))

def get_converted_locations_data(item):
    if not item:
        return []
    return [
        {
            "location_fullpath": item['locationhierarchy'],
            "length": 1,
            "location_name": item['locationhierarchy'],
            "parent_full_path": item['locationhierarchy'],
            "parent_name": item['locationhierarchy']
        },
        {
            "location_fullpath": get_full_path([item['locationhierarchy'], item['locationname']]),
            "length": 2,
            "location_name": item['locationname'],
            "parent_full_path": item['locationhierarchy'],
            "parent_name": item['locationhierarchy']
        },
        {
            "location_fullpath": get_full_path([item['locationhierarchy'], item['locationname'], item['workstate']]),
            "length": 3,
            "location_name": item['workstate'],
            "parent_full_path": get_full_path([item['locationhierarchy'], item['locationname']]),
            "parent_name": item['locationname']
        },
        {
            "location_fullpath": get_full_path([item['locationhierarchy'],
                                                                item['locationname'], item['workstate'], item['workcity']]),
            "length": 4,
            "location_name": item['workcity'],
            "parent_full_path": get_full_path([item['locationhierarchy'], item['locationname'], item['workstate']]),
            "parent_name": item['workstate']
        }
    ]

def get_dept_data(response):
    response = response['rows']
    if response:
        return list(map(lambda x:{
            'name': x['cells'][0]['textValue'],
            'enabled': x['cells'][1]['textValue'],
            'uri': x['cells'][0]['uri']
        }, response))

def get_uri_to_enable():
    disabled_users = rail.load_all_records(rail.result('query_users_to_enable'))
    users_to_enable = []
    for data in disabled_users:
        daydiff = (datetime.today() - datetime.strptime(data['returndatefromleave'], "%m/%d/%Y")).days
        if daydiff > 0:
            users_to_enable.append({"uri": data['uri']})
    return json.dumps(users_to_enable)
