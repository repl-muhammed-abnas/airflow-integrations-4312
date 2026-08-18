from datetime import datetime as dt, timedelta
import rail
from dxctechnology.australia_termination_balance_v2.mapper.time_off_balance_mapper import Time_off_mappper, Disabled_user_timeoffs


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_start_date_begin_of_week(config):
    cut_off_date = dt.strptime("2023-07-01", "%Y-%m-%d")
    start_date = (dt.utcnow() - timedelta(days=dt.utcnow().weekday())) - timedelta(days=84)
    if start_date < cut_off_date and (config.company_key).lower() == 'dxctechnology':
        return cut_off_date.strftime("%Y-%m-%d")
    return start_date.strftime("%Y-%m-%d")


def get_end_date_begin_of_week():
    return str(dt.utcnow().strftime("%Y-%m-%d"))


def get_utc_now_date_string(config):
    return dt.utcnow().strftime(config.date_time_format)


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_run_user_report_payload():
    get_specific_report_details = rail.result('get_user_report_details')
    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_run_us_user_report_payload(dag_run):
    get_specific_report_details = rail.result('get_user_report_details')
    filter_values = []

    for item in dag_run.conf['division_name']:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_report_details'
                            )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri'),
            "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                            ), "displayText", item, "uri")).split(':')[-1] if rail.result("get_all_enabled_divisions") else None,
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
    get_specific_report_details = rail.result(
        'get_termination_balance_report_details')
    filter_values = []
    users_data = rail.load_all_records(rail.result('query_all_users_data'))
    for item in users_data:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                            )['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri'),
            "value": item['id']
        })

    for timeofftype in dag_run.conf['timeofftype_uris']:
        if timeofftype:
            filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                                )['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri'),
                "value": timeofftype.split(':')[-1] if timeofftype else None
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


def get_termination_balance_us_data_row(items):
    get_user_data_in_query = list(filter(lambda item: item['emp_id']== items['Employee_ID'], map(lambda item:{
        'emp_id': item['Employee_ID'],
        'timeoff_balance': item['Time_Off_Balance']
    },rail.load_all_records(rail.result("query_active_user_balance_data")))))

    time_off_balance = list(filter(lambda x:  x['Time off type'] == items['Time_Off_Type'], Time_off_mappper))

    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['Employee_ID']

    timeoff_balance = items['Time_Off_Balance']
    if get_user_data_in_query:
        timeoff_balance = str(float(get_user_data_in_query[0]['timeoff_balance'].replace(",","")) + float(items['Time_Off_Balance'].replace(",","")))

    return ["P2006",
            personnelnumber,
            "AU",
            "",
            "INS",
            "2006",
            time_off_balance[0]['Quote Type'],
            dt.strptime(items['User_End_Date'], "%d %B %Y").strftime("%Y%m%d") if items['User_End_Date'] else "",
            "99991231",
            "",
            "",
            "",
            "",
            "",
            "",
            time_off_balance[0]['Quote Type'],
            timeoff_balance.split("- ")[-1].replace(",",""),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
            ]


def get_formated_user_row(item):
    return {
        'username': item['User Name'],
        'location': item['Location (Current)'],
        'useruri': item['UserUri'],
        # 2022-08-30 - for sql date format
        'userenddate': dt.strptime(item['User End Date'], "%d %B %Y").strftime("%Y-%m-%d") if item['User End Date'] else None,
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


def remove_delimiter():
    return is_upload_data_to_sftp_failed()

# pylint: disable=line-too-long
def terminationbalance_gsap_child_conf(config):
    time_off_list= Disabled_user_timeoffs
    time_off_type_uris=[]

    for time_off_type in time_off_list:
        time_off_type_uris.append(rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                    ), "displayText", time_off_type, "uri"))

    return {
        'location': 'AUS',
        'file_format': "AUS_EXPORT",
        'division_name': config.division_name_gsap,
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"
                                                    ), "displayText", "Australia", "uri")),
        'timeofftype_uris': time_off_type_uris,
        'dag_start_date_time': get_utc_now_date_string(config),
        'user_report_name': config.aus_user_report_name,
        'termination_balance_report_name': config.termination_balance_report_name_us,
        'file_name': config.file_name_prefix + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_AUREPL_REGS01_DUT8G2I",
        'encyrpt_file': config.encrypt_output_file
    }

def terminationbalance_es_child_conf(config):
    time_off_list= Disabled_user_timeoffs
    time_off_type_uris=[]

    for time_off_type in time_off_list:
        time_off_type_uris.append(rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                    ), "displayText", time_off_type, "uri"))

    return {
        'location': 'AUS',
        'file_format': "AUS_EXPORT",
        'division_name': config.division_name_es,
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"
                                                    ), "displayText", "Australia", "uri")),
        'timeofftype_uris': time_off_type_uris,
        'dag_start_date_time': get_utc_now_date_string(config),
        'user_report_name': config.aus_user_report_name,
        'termination_balance_report_name': config.termination_balance_report_name_us,
        'file_name': config.file_name_prefix + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_AUREPL_RECP01_DUT8G2I",
        'encyrpt_file': config.encrypt_output_file
    }
