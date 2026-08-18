from datetime import datetime as dt
import pendulum as pd
import rail

SQL_DATE_FORMAT = "%Y-%m-%d"
REPORT_DATE_FORMAT = "%d %B %Y"
MDY_DATE_FORMAT = "%m/%d/%Y"
YMD_DATE_FORMAT = "%Y%m%d"
HMS_DATE_FORMAT = "%H%M%S"
null = None

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_logging_details(time_zone, date_time_format, duration_days):
    now = pd.now(time_zone)
    start_date = now.start_of('week').subtract(days=duration_days)
    return {
        "current_date_mdy": now.strftime(MDY_DATE_FORMAT),
        "current_date_ymd": now.strftime(YMD_DATE_FORMAT),
        "current_time_hms": now.strftime(HMS_DATE_FORMAT),
        "dag_start_date_time": now.strftime(date_time_format),
        "start_date": start_date.strftime(SQL_DATE_FORMAT),
        "end_date": now.strftime(SQL_DATE_FORMAT),
        "start_date_json": {
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day
        },
        "end_date_json": {
            "year": now.year,
            "month": now.month,
            "day": now.day
        }
    }

def get_location_company_data_conf(config, item):
    return {
        'region': item["region"],
        'location': item["location"],
        'location_code': item["location_code"],
        'location_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_specific_locations"
            ), "displayText", item["location"], "uri"),
        'timeoff_types': list(map(lambda timeoff_item: {
            "timeoff_type_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_specific_timeoff_types"), "displayText", timeoff_item["leave_type"], "uri"),
            "timeoff_type_name": timeoff_item["leave_type"],
            "paycode": timeoff_item["wage_code"],
            "info_type": timeoff_item["info_type"]
        }, item["timeoff_types"])),
        'dag_start_date_time': rail.result("logging_details")["dag_start_date_time"],
        'users_report_name': item["users_report_name"],
        'termination_balance_report_name': item["termination_balance_report_name"],
        'sequence_no': item['sequence_no'],
        'file_name': config.file_name_prefix + "_" + str(pd.now(config.time_zone).strftime("%Y%m%d%H%M%S")) + "_" + item["location_code"] + "REPL_REPL" + item['sequence_no'] +"_DUT8G2I",
        'encrypt_file': item["encrypt"],
        'logging_details': rail.result("logging_details")
    }

def get_region_location_combination(locations_company_codes_mapper):
    seen = set()
    unique_items = []
    for item in locations_company_codes_mapper:
        key = (item.get("location"), item.get("region"))
        if key[0] and key[1] and key not in seen:
            seen.add(key)
            unique_items.append({"location": key[0], "region": key[1]})

    return unique_items

def get_run_user_report_payload(dag_run, locations_company_codes_mapper):
    get_specific_report_details = rail.result('get_user_report_details')
    current_division_filter = rail.find_first_by_attr_and_get_attr(
        get_specific_report_details['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri')
    filter_values = []
    for item in locations_company_codes_mapper:
        if item['location'] == dag_run.conf['location'] and item['region'] == dag_run.conf['region']:
            filter_values.append({
                "reportFilterUri": current_division_filter,
                "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                    ), "displayText", item['company_code'], "uri")).split(':')[-1] if rail.result("get_all_enabled_divisions") else null,
            })
    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_run_termination_balance_report_payload(dag_run):
    get_specific_report_details = rail.result('get_termination_balance_report_details')
    filter_values = []
    users_data = rail.load_all_records(rail.result('query_all_users_data'))
    user_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
        )['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri')
    timeoff_type_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
        )['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri')
    for item in users_data:
        filter_values.append({
            "reportFilterUri": user_filter_uri,
            "value": item['id']
        })
    filter_values.extend([{
        "reportFilterUri": timeoff_type_filter_uri,
        "value": timeoff_type['timeoff_type_uri'].split(':')[-1]
    } for timeoff_type in dag_run.conf['timeoff_types']])

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def getReportPayload(response):
    data = response.json()['d']
    return data['payload']

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_final_users_data_row(items):
    return [items['username'], items['location'],
            items['useruri'],
            items['useruri'].split(':')[-1]
            ]

def get_termination_balance_data_row(item):
    personnelnumber = ""
    if item['Actual_Employee_ID']:
        personnelnumber = item['Actual_Employee_ID']
    else:
        personnelnumber = item['Employee_ID']
    paycode = rail.find_first_by_attr_and_get_attr(get_conf()['timeoff_types'], 'timeoff_type_name', item['Time_Off_Type'], 'paycode')
    info_type = rail.find_first_by_attr_and_get_attr(get_conf()['timeoff_types'], 'timeoff_type_name', item['Time_Off_Type'], 'info_type')
    user_enddate = dt.strptime(item['User_End_Date'], REPORT_DATE_FORMAT).strftime(YMD_DATE_FORMAT) if item['User_End_Date'] else ""
    header = ("P" + info_type) if info_type else "P2010"
    return [
        header,
        personnelnumber,
        get_conf()["location_code"],
        "",
        "INS",
        info_type,
        paycode,
        user_enddate,
        user_enddate,
        "",
        "",
        "",
        "",
        paycode,
        "",
        "",
        "",
        "",
        "",
        item['Time_Off_Balance'],
        "001"
    ]

def get_formated_user_row(item):
    return {
        'username': item['User Name'],
        'location': item['Location (Current)'],
        'useruri': item['UserUri'],
        # 2022-08-30 - for sql date format
        'userenddate': dt.strptime(item['User End Date'], REPORT_DATE_FORMAT).strftime(SQL_DATE_FORMAT) if item['User End Date'] else null,
        'exported': item['TermExportedAUS']
    }.values()

def is_upload_data_to_sftp_failed():
    if get_task_state('upload_export_data_to_sftp') == 'failed':
        return True
    return False

def is_upload_log_to_sftp_failed():
    if get_task_state('upload_log_data_to_sftp') == 'failed':
        return True
    return False
