from datetime import datetime as dt, timedelta
import rail
from dxctechnology.lcsc_les_us_termination_balance_v3.mapper.company_code_mapper_usles_uscsc import COMPANY_CODE_MAP_US


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_start_date_begin_of_week():
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    return str((begin_of_week - timedelta(days=3)).strftime("%Y-%m-%d"))


def get_end_date_begin_of_week():
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    return str((begin_of_week + timedelta(days=5)).strftime("%Y-%m-%d"))


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

def get_run_us_user_report_payload():
    get_specific_report_details = rail.result('get_user_report_details')
    filter_values = []
    for item in COMPANY_CODE_MAP_US:
        if item['File format'] == get_dag_run_conf()['file_format']:
            filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_report_details'
                                )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri'),
                "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                                ), "displayText", item['Company code'], "uri")).split(':')[-1] if rail.result("get_all_enabled_divisions") else None,
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
    users_data = get_data_from_document(rail.result('query_all_users_data'))
    for item in users_data:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                                                                                )['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri'),
            "value": item['id']
        })
    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                            )['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri'),
        "value": dag_run.conf['timeofftype_1'].split(':')[-1] if dag_run.conf['timeofftype_1'] else None
    })
    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                                    )['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri'),
        "value": dag_run.conf['timeofftype_2'].split(':')[-1] if dag_run.conf['timeofftype_2'] else None
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

    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['Employee_ID']
    return ["P2010",
            personnelnumber,
            "US",
            "",
            "INS",
            "2010",
            items['Time_Off_Type'].split("- ")[-1] if items['Time_Off_Type'] else "",
            dt.strptime(items['User_End_Date'], "%d %B %Y").strftime("%Y%m%d") if items['User_End_Date'] else "",
            dt.strptime(items['User_End_Date'], "%d %B %Y").strftime("%Y%m%d") if items['User_End_Date'] else "",
            "",
            "",
            "",
            "",
            items['Time_Off_Type'].split("- ")[-1] if items['Time_Off_Type'] else "",
            "",
            "",
            "",
            "",
            "",
            items['Time_Off_Balance'].split("- ")[-1],
            "001"
            ]


def get_formated_user_row(item):
    return {
        'username': item['User Name'],
        'location': item['Location (Current)'],
        'useruri': item['UserUri'],
        # 2022-08-30 - for sql date format
        'userenddate': dt.strptime(item['User End Date'], "%d %B %Y").strftime("%Y-%m-%d") if item['User End Date'] else None,
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


def terminationbalance_usa_les_child_conf(config):
    return {
        'location': 'US',
        'file_format': "US_LES",
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"
                                                                          ), "displayText", "United States of America", "uri")),
        'timeofftype_1': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                                           ), "displayText", config.timeoff_type1_name_USLes, "uri")),
        'timeofftype_2': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                                           ), "displayText", config.timeoff_type2_name_USLes, "uri")),
        'dag_start_date_time': get_utc_now_date_string(config),
        'user_report_name': config.usa_les_user_report_name,
        'termination_balance_report_name': config.termination_balance_report_name_us,
        'file_name': config.file_name_prefix_US + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_USREPL_REPL02_DUT8G2I"  + ".SAP",
        'encyrpt_file': config.encrypt_output_file_usa
    }
def terminationbalance_usa_csc_child_conf(config):
    return {
        'location': 'US',
        'file_format': "US_CSC",
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"
                                                                          ), "displayText", "United States of America", "uri")),
        'timeofftype_1': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                                           ), "displayText", config.timeoff_type1_name_USCsc, "uri")),
        'timeofftype_2': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                                           ), "displayText", config.timeoff_type2_name_USCsc, "uri")),
        'dag_start_date_time': get_utc_now_date_string(config),
        'user_report_name': config.usa_csc_user_report_name,
        'termination_balance_report_name': config.termination_balance_report_name_us,
        'file_name': config.file_name_prefix_US + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_USREPL_REPL01_DUT8G2I"  + ".SAP",
        'encyrpt_file': config.encrypt_output_file_usa
    }
