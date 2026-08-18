from datetime import datetime as dt
import json
import functools
import rail

def get_invalid_logs_property_conf(item):
    mandatory_fields = ['schedulename', 'empid', 'startdate', 'starttime','endtime']
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "employeeid": item['empid'],
        "schedulename": item['schedulename'],
        "startdate": item['startdate'],
        "status": "Skipped",
        "action": "Validation",
        "details": get_missing_field() + " not present in feed file",
        "ecid": '{{ dag_run_ecid() }}'
    }

def get_invalid_users_conf(item):
    return {
        "employeeid": item['empid'],
        "schedulename": item['schedulename'],
        "startdate": item['startdate'],
        "status": "Skipped",
        "action": "Validation",
        "details": "User uri not present, user not found/disabled in replicon",
        "ecid": '{{ dag_run_ecid() }}'
    }

def get_shift_data(response):
    return list(map(lambda item:{
        'name': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'description': item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        'break_hours': (item['cells'][3]['textValue'] != '0.00')
    },response['rows']))

def get_query_data():
    data = rail.load_all_records(rail.result("query_shift_schedule_details"))[0]
    return {
        'name': data['schedulename'],
        'code': data['schedulecode'],
        'description': data['description'],
        'empid': data['empid'],
        'startdate': dt.strptime(data['startdate'], '%m/%d/%Y').strftime("%Y-%m-%d") if data['startdate'] else None,
        'startTime': data['starttime'],
        'endTime': data['endtime'],
        'break1': data['break1'],
        'break1_start_time': data['break1starttime'],
        'break1_duration': data['break1duration'],
        'break2': data['break2'],
        'break2_start_time': data['break2starttime'],
        'break2_duration': data['break2duration'],
    }

def check_shift_name(dag_run):
    return dag_run.conf['schedulename'] == rail.result("get_query_data")['name']

def get_replicon_shift_data(response):
    return list(map(lambda item: {
        'name': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri']
    },response['rows']))

def check_shift_description():
    return rail.result("shift_details_in_replicon")[0]['description'] == rail.result("get_query_data")['description']

def get_shift_details(response):
    return list(map(lambda item:{
        'name': item['breakType']['displayText'],
        'duration_hr': item['duration']['hours'],
        'duration_min': item['duration']['minutes'],
        'duration_sec': item['duration']['seconds'],
        'start_time_hr': item['inTime']['hour'],
        'start_time_min': item['inTime']['minute']
    },response[0]["shiftDetails"]["breakSegments"]))

@functools.lru_cache(maxsize=128)
def get_report_data():
    return rail.load_all_records(rail.result("users_report_data_collection"))

def get_required_data(item):
    query_data = get_report_data()
    return [
        item['schedulename'],
        item['schedulecode'],
        item['description'],
        item['empid'],
        dt.strptime(item['startdate'], '%m/%d/%Y').strftime("%Y-%m-%d") if item['startdate'] else None,
        item['starttime'],
        item['endtime'],
        rail.find_first_by_attr_and_get_attr(query_data,'Employee_ID',item['empid'],'UserUri'),
        rail.find_first_by_attr_and_get_attr(query_data,'Employee_ID',item['empid'],'User_Status'),
        rail.find_first_by_attr_and_get_attr(query_data,'Employee_ID',item['empid'],'Schedule_Name__Current_'),
        rail.find_first_by_attr_and_get_attr(query_data,'Employee_ID',item['empid'],'User_Start_Date'),
        rail.find_first_by_attr_and_get_attr(query_data,'Employee_ID',item['empid'],'User_End_Date')
    ]

def get_assigned_shift_dates():
    shift_details = rail.result("get_shift_schedule_summary")
    data = rail.result("get_query_data")
    shift_result = []
    pto_result = []

    for idx, item in enumerate(shift_details):
        shift_result.append(item)
        check = rail.find_first_by_attr_and_get_attr(data,'startdate',item['date'],'schedulename')
        if check:
            shift_result[idx]['delete_shift'] = 'yes'
        else:
            shift_result[idx]['delete_shift'] = 'no'

    for idx, item in enumerate(data):
        pto_result.append(item)
        check = rail.find_first_by_attr_and_get_attr(shift_details,'date',item['startdate'],'name')
        if check:
            pto_result[idx]['shift_assigned'] = 'yes'
        else:
            pto_result[idx]['shift_assigned'] = 'no'

    return {
        'shift_result': shift_result,
        'pto_result': pto_result
    }

def filter_shifts(response):
    return list(map(lambda item: {
        'name': item['shift']['displayText'],
        'date': (dt(item['date']['year'], item['date']['month'],item['date']['day'])).strftime("%Y-%m-%d"),
        'assignmenturi': item['assignmentUri']
    },response))

def check_any_shifts_to_be_deleted():
    shift_assignments_data = rail.result("get_assigned_shift_dates")['shift_result']
    return bool(list(filter(lambda shift_data: shift_data['delete_shift'] == 'yes', shift_assignments_data)))

def check_any_shifts_to_be_created():
    shift_assignments_data = rail.result("get_assigned_shift_dates")['pto_result']
    return bool(list(filter(lambda shift_data: shift_data['shift_assigned'] == 'no', shift_assignments_data)))

def do_format_logs():
    master_log = json.loads(rail.result('load_master_log'))

    users = list(map(lambda x: {
        'empid': x['properties'].get('employeeid', ''),
        'date': x['properties'].get('startdate', ''),
        'name': x['properties'].get('schedulename', '')
        }, master_log))

    final_data = list({f"{value['empid']}*{value['date']}*{value['name']}": value for value in users}.values())

    logs = []
    # pylint: disable=cell-var-from-loop
    for item in final_data:
        user_logs = list(
            filter(lambda x: x['properties'].get('employeeid', '') == item['empid'] and x[
                'properties'].get('startdate', '') == item['date'] and x['properties'].get(
                    'schedulename', '') == item['name'], master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'employeeid': first['properties']['employeeid'],
                'schedulename': first['properties']['schedulename'],
                'startdate': first['properties']['startdate'],
                'status': first['properties']['status'],
                'action': first['properties']['action'],
                'details': ', '.join(list(map(lambda x: x['properties'].get('details'), user_logs))),
                'ecid': first['properties']['ecid'],
            })
    return json.dumps(logs, ensure_ascii=False)
