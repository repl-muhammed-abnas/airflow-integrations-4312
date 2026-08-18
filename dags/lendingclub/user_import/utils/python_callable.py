# pylint: disable=unused-variable
from datetime import datetime
import json
import rail
from lendingclub.user_import.utils.request_payload import get_today_dateformat_payload

def get_today():
    return datetime.now().strftime("%m/%d/%Y")

def get_uri_data_on_code(response, arg_code, argcode, arg_uri, arg_name):
    filtered_data =  list(filter(lambda x: x['cells'][0]['textValue'] == argcode , response['rows']))
    if filtered_data:
        return list(filter(lambda x: x[arg_code] == argcode , list(map(lambda d: {
            arg_uri : d['cells'][1]['uri'],
            arg_name : d['cells'][1]['textValue'],
            arg_code : d['cells'][0]['textValue']
        }, response['rows']))))[0]

    return {
        arg_uri : None,
        arg_name : None
    }

def get_uri_data_on_name(response, argname, arg_uri, arg_name):
    filtered_data =  list(filter(lambda x: x['cells'][1]['textValue'] == argname , response['rows']))
    if filtered_data:
        return list(filter(lambda x: x[arg_name] == argname , list(map(lambda d: {
            arg_uri : d['cells'][1]['uri'],
            arg_name : d['cells'][1]['textValue']
        }, response['rows']))))[0]
    return {
        arg_uri : None,
        arg_name : None
    }

def get_failure_list(dag_run):
    error_list = []
    mandatory_fields = ['firstname', 'lastname', 'employeestatus', 'email', 'loginname', 'employeetypename', 'department', 'hiredate']
    filtered_data = list(filter(lambda x: dag_run.conf[x] == '', mandatory_fields))
    if filtered_data:
        list(map(lambda d : error_list.append(d + ' not present'), list(filter(
            lambda x: dag_run.conf[x] == '', mandatory_fields))))
    error_str = ','.join(error_list)
    if len(error_list) > 0:
        return {
            "error_value" : error_str,
            "error_status" : True
        }
    return {
        "error_value" : error_str,
        "error_status" : False
    }

def get_userdata_list_for_managerid(response, dag_run):
    userdata = response['rows']
    filtered_data = list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['managerid'], userdata))
    if filtered_data:
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['managerid'], list(map(
            lambda d:{
                'uri' : d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt' : d['cells'][0]
                } , userdata))))
    return []

def get_uri_value(response):
    if not response['rows']:
        return []
    return list(set(map(lambda data: data['cells'][1]['uri'], response['rows'])))[0]

def get_timeofftypeuris(response):
    time_offtypeuri_list = []
    if len(response) > 0:
        list(map(lambda x: time_offtypeuri_list.append(x['uri']), response))
    return time_offtypeuri_list

def get_status_and_details_for_add(dag_run):
    message = "Success"
    details = "User added successfully"
    has_exception_message = ','.join(list(map(lambda v: v['value'] , rail.load_all_records(rail.result('write_log_user_import')))))
    if has_exception_message:
        message = "Exception"
        details = "Partially Added" + ' ' + has_exception_message
    return {
        "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
        "Action": "Adduser",
        "Status": message,
        'Details': details
    }

def check_start_date_mismatch(dag_run):
    user_start_date = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    start_date_value = str(user_start_date['year']) + '-' + str(user_start_date['month']) + '-' + str(user_start_date['day'])
    if datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d') != datetime.strptime(start_date_value, '%Y-%m-%d'):
        return True
    return False

def get_user_job_level_value():
    data = rail.result('get_user_data')[0]['userDetails']['customFieldValues']
    return rail.find_first_by_attr_and_get_attr(data, 'customField.name', 'Job Level', 'text', '')

def get_user_vendor_value():
    data = rail.result('get_user_data')[0]['userDetails']['customFieldValues']
    return rail.find_first_by_attr_and_get_attr(data, 'customField.name', 'Vendors', 'text', '')

def get_supervisor_data():
    datatype_val =  rail.result('get_user_details')['rows'][0]['cells'][0]['dataType']
    if datatype_val == 'urn:replicon:list-type:null':
        return {
            "datatype" : datatype_val,
            "assigned_supervisor_uri" : ''
        }
    return {
        "datatype" : datatype_val,
        "assigned_supervisor_uri" : rail.result('get_user_details')['rows'][0]['cells'][0]['uri']
    }

def get_schedule_list(schedule_val, schd, assigned_uri):
    data = rail.result('get_user_data')[0][schedule_val]
    schedule_list = []
    req_schd = {}
    req_schd[schd] = {}
    req_schd['effectiveDate'] = {}
    for i, loc in enumerate(data):
        schedule_dict = {}
        schedule_dict[schd] = {}
        schedule_dict['effectiveDate'] = {}
        if loc['effectiveDate'] is not None:
            effectivedate = str(loc['effectiveDate']['month']) + '/' + str(loc['effectiveDate']['day']) + '/' + str(loc['effectiveDate']['year'])
            if effectivedate != str(get_today()):
                schedule_dict[schd]['uri'] = loc[schd]['uri']
                schedule_dict['effectiveDate']['day'] = loc['effectiveDate']['day']
                schedule_dict['effectiveDate']['month'] = loc['effectiveDate']['month']
                schedule_dict['effectiveDate']['year'] = loc['effectiveDate']['year']
                schedule_list.append(schedule_dict)

        else:
            schedule_dict[schd]['uri'] = loc[schd]['uri']
            schedule_dict['effectiveDate'] = loc['effectiveDate']
            schedule_list.append(schedule_dict)

    req_schd[schd]['uri'] = assigned_uri
    req_schd['effectiveDate']['day'] = int(get_today_dateformat_payload()['day'])
    req_schd['effectiveDate']['month'] = int(get_today_dateformat_payload()['month'])
    req_schd['effectiveDate']['year'] = int(get_today_dateformat_payload()['year'])

    schedule_list.append(req_schd)

    return schedule_list

def get_timesheet_template_to_assign(dag_run):
    if 'Full Time Employee' in dag_run.conf['employeetypename']:
        return 'Full time employees'
    if 'Part Time Employee' in dag_run.conf['employeetypename']:
        return 'Part time/Contractor employees'
    if 'Contractor' in dag_run.conf['employeetypename']:
        return 'Part time/Contractor employees'
    return None

def get_status_and_details_for_update(dag_run):
    has_success_message = ','.join(list(map(lambda v: v['status'] , rail.load_all_records(rail.result('write_log_success_import')))))
    message = "Success"
    details = "User updated successfully" + ' ' + has_success_message
    has_exception_message = ','.join(list(map(lambda v: v['value'] , rail.load_all_records(rail.result('write_log_user_import')))))
    if has_exception_message:
        message = "Exception"
        details = "Partially Updated" + ' ' + has_exception_message + ' ' + has_success_message
    return {
        "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
        "Action": "Update",
        "Status": message,
        'Details': details
    }

def do_format_logs():
    def can_filter_record(log):
        if log['Status'].lower() == "error" and log['Action'].lower() == "add" and log['user_uri']:
            return True
        if log['Status'].lower() in ['success', 'exception'] and log['Action'].lower() == "add":
            return True
        return False

    def get_filtered_records(logs, status):
        return list(filter(lambda log: log['Status'].lower() == status, logs))

    def get_record_summary(logs):
        return {
            "success": len(get_filtered_records(logs, 'success')),
            "failed":  len(get_filtered_records(logs, 'error')),
            "exception": len(get_filtered_records(logs, "exception")),
            "new_users_added": len(list(filter(can_filter_record, logs))),
            "users_updated": len(list(filter(lambda log: log['Status'].lower() in ['success', 'exception', 'error']
                                             and log['Action'].lower() == "update", logs)))
        }

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['Status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"

    master_log = json.loads(rail.result('load_master_log'))

    users = list(
        set(map(lambda x: x['properties'].get('UserID', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for user_id in users:
        if not user_id:
            continue
        user_logs = list(
            filter(lambda x: x['properties'].get('UserID', '') == user_id and x['properties'].get('Details', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'UserID': user_id,
                'Action': first['properties'].get('Action'),
                'Status': get_status(user_logs),
                'Details': ",".join(list(map(lambda x: x['properties'].get('Details'), user_logs))),
                "ecid": first['ecid']
            })

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }

def get_exceptions():
    exceptions = (rail.result('log_supervisor_not_assigned') if rail.result(
        'log_supervisor_not_assigned') else '') + (rail.result('log_supervisor_absent') if rail.result(
            'log_supervisor_absent') else '') + (rail.result('log_no_supervisor_loginname') if rail.result(
            'log_no_supervisor_loginname') else '') + (rail.result('log_supervisor_equals_user') if rail.result(
            'log_supervisor_equals_user') else '')
    return exceptions
