from datetime import datetime as dt
import json
import functools
import pandas as pd
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
        "details": get_missing_field() + " not present in feed file"
    }

def get_invalid_users_conf(item):
    return {
        "employeeid": item['empid'],
        "schedulename": item['schedulename'],
        "startdate": item['startdate'],
        "status": "Skipped",
        "action": "Validation",
        "details": "User uri not present, user not found/disabled in replicon"
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
    log_artifacts = []
    log_records = []

    userlogs = rail.result("create_log")

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

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

    return final_log_records

def get_date_diff(time1,time2):
    t1format = int((dt.strptime(time1,'%I:%M %p')).strftime('%H'))
    t2format = int((dt.strptime(time2,'%I:%M %p')).strftime('%H'))

    return t2format - t1format

def process_single_employee(schedule_data):
    grouped = schedule_data.groupby('startdate')
    result_rows = []
    skipped_rows = []
    extra_rows= []

    def add_items_to_list(list_name,schedule_name,shift_row,end_date_row,message = None):
        return list_name.append({
            "schedulename": schedule_name,
            "schedulecode": shift_row['schedulecode'],
            "description": shift_row['description'],
            "empid": shift_row['empid'],
            "startdate": date,
            "starttime": shift_row['starttime'],
            "endtime": end_date_row['endtime'],
            "useruri": shift_row['useruri'],
            "userstatus": shift_row['userstatus'],
            "schedule": shift_row['schedule'],
            "user_startdate": shift_row['user_startdate'],
            "user_enddate": shift_row['user_enddate'],
            "message": message
        })

    for date, group in grouped:
        shift_rows = group[group['schedulename'].str.contains('CTR: SHIFT')]
        extra_billable_rows = group[group['schedulename'].str.contains('CTR: EXTRA HOURS BILLABLE')]
        absence_rows = group[group['schedulename'].str.contains('UPLEXHR: ABSENCE DURING EXHRS')]

        shift_row = shift_rows.iloc[0] if not shift_rows.empty else None
        absence_row = absence_rows.iloc[0] if not absence_rows.empty else None
        extra_billable_row = extra_billable_rows.iloc[0] if not extra_billable_rows.empty else None

        if not shift_rows.empty and not extra_billable_rows.empty:
            if get_date_diff(extra_billable_row['starttime'],extra_billable_row['endtime']) <= 4 and get_date_diff(shift_row['starttime'],shift_row['endtime']) <= 8:
                add_items_to_list(result_rows,'CTR: SHIFT',shift_row,extra_billable_row,'Shift Added to User Successfully')
                add_items_to_list(extra_rows,'CTR: SHIFT',shift_row,shift_row,'Shift Added to User Successfully')
                add_items_to_list(extra_rows,'CTR: EXTRA HOURS BILLABLE',extra_billable_row,extra_billable_row,'Shift Added to User Successfully')
            elif get_date_diff(shift_row['starttime'],shift_row['endtime']) > 8:
                add_items_to_list(skipped_rows,'CTR: SHIFT',shift_row,shift_row,
                                  'User not assigned with regular shift as "CTR:SHIFT" hours received more than 8 hours')
                add_items_to_list(skipped_rows,'CTR: EXTRA HOURS BILLABLE',extra_billable_row,extra_billable_row,
                                  "User not assigned with extra billable hours as 'CTR:SHIFT' hours received more than 8 hour")
            else:
                add_items_to_list(extra_rows,'CTR: SHIFT',shift_row,shift_row,'Shift Added to User Successfully')
                add_items_to_list(result_rows,'CTR: SHIFT',shift_row,shift_row,'Shift Added to User Successfully')
                add_items_to_list(skipped_rows,'CTR: EXTRA HOURS BILLABLE',extra_billable_row,extra_billable_row,
                                  "User not assigned with extra billable hours as billable hours received more than 4 hours")
        elif not shift_rows.empty and not absence_rows.empty:
            add_items_to_list(result_rows,'CTR: SHIFT',shift_row,absence_row,'Shift Added to User Successfully')
            add_items_to_list(extra_rows,'CTR: SHIFT',shift_row,shift_row,'Shift Added to User Successfully')
            add_items_to_list(extra_rows,'UPLEXHR: ABSENCE DURING EXHRS',absence_row,absence_row,'Shift Added to User Successfully')

        elif (shift_rows.empty and not absence_rows.empty) or (shift_rows.empty and not extra_billable_rows.empty):
            add_items_to_list(skipped_rows,'UPLEXHR: ABSENCE DURING EXHRS',absence_row,absence_row,
                              'Regular shift is not assigned to the user since the "CTR:SHIFT" is not received in the feed file.') if not absence_rows.empty else None
            add_items_to_list(skipped_rows,'CTR: EXTRA HOURS BILLABLE',extra_billable_row,extra_billable_row,
                              'Regular shift is not assigned to the user since the "CTR:SHIFT" is not received in the feed file.') if not extra_billable_rows.empty else None
        else:
            add_items_to_list(result_rows,'CTR: SHIFT',shift_row,shift_row,'Shift Added to User Successfully')
            add_items_to_list(extra_rows,'CTR: SHIFT',shift_row,shift_row,'Shift Added to User Successfully')

    rail.set_result(key="skipped_records",val= skipped_rows)
    rail.set_result(key="extra_records",val= extra_rows)

    return pd.DataFrame(result_rows)

def get_shifts_to_assign(schedule_data):
    df = pd.DataFrame(schedule_data)
    return process_single_employee(df).to_dict(orient='records')
