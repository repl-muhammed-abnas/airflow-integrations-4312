from datetime import datetime as dt
import pendulum
import rail
from dateutil.relativedelta import relativedelta
# pylint: disable=no-name-in-module


def get_start_date_begin_of_week():
    start_date = dt.strptime(dt.utcnow().strftime(
        "%Y-%m-%d"), "%Y-%m-%d") + relativedelta(months=-6)
    return dt.strftime(start_date, "%Y-%m-%d")


def get_end_date_begin_of_week():
    return dt.utcnow().strftime("%Y-%m-%d")


def get_utc_now_date_string(config):
    return dt.utcnow().strftime(config.date_time_format)


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

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_run_termination_balance_report_payload():
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
    return response.json()['d']['payload']


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_final_users_data_row(items):
    return [items['username'], items['location'],
            items['useruri'],
            items['useruri'].split(':')[-1]
            ]


def get_termination_balance_us_data_row(items):

    personnelnumber = items['Employee_ID']

    timeoff_balance = items['Time_off_Balance']

    if items['Time_Off_Type'] in ['[CAN] Heures supplémentaires cumulées/Time off in Lieu','[CAN] Heures supplémentaires cumulées/Time off in Lieu SC']:
        paycode = "2058"
    else:
        paycode = "2302"

    return ["P2010",
            personnelnumber,
            "CA",
            "000",
            "INS",
            "2010",
            paycode,
            dt.strptime(items['User_End_Date'], "%b %d, %Y").strftime(
                "%Y%m%d") if items['User_End_Date'] else "",
            dt.strptime(items['User_End_Date'], "%b %d, %Y").strftime(
                "%Y%m%d") if items['User_End_Date'] else "",
            "",
            "",
            "000",
            "",
            paycode,
            "",
            "",
            "",
            "",
            "CAD",
            timeoff_balance.split("- ")[-1].replace(",", ""),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
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
        'userstartdate': dt.strptime(item['User Start Date'], "%b %d, %Y").strftime("%Y-%m-%d") if item['User Start Date'] else None,
        'userenddate': dt.strptime(item['User End Date'], "%b %d, %Y").strftime("%Y-%m-%d") if item['User End Date'] else None,
        'exported': item['Term Exported'],
        'timeofftemplate': item['Time Off Template']
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

    return {
        'location': 'Canada',
        'file_format': "Canada ADP Export",
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"
                                                                          ), "displayText", "CAN", "uri")),
        'dag_start_date_time': get_utc_now_date_string(config),
        'user_report_name': config.user_report_name,
        'termination_balance_report_name': config.termination_balance_report_name,
        'file_name': "P" + config.adp_gv_system + config.gv_system_number + "476" + "_" + dt.utcnow().strftime("%Y%m%d%H%M%S") + "_CATIME_HRMD"+get_sequence(config.CANADA_PAYROLL_CALENDER_MAPPER_TO_USE, config.time_zone)+"_DUT8G2I",
        'encyrpt_file': config.encrypt_output_file
    }


def get_sequence(calendra_mapper, time_zone):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    return "03" if bool(rail.find_first_by_attr_and_get_attr(
        calendra_mapper, "payroll_processing_date", current_date)) else "02"
