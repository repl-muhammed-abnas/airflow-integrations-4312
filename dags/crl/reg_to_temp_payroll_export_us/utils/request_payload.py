from datetime import datetime as dt
import pendulum
import rail
from dateutil.relativedelta import relativedelta
# pylint: disable=no-name-in-module


def get_start_date_begin_of_week():
    start_date = dt.strptime(dt.now().strftime(
        "%Y-%m-%d"), "%Y-%m-%d") + relativedelta(months=-6)
    return dt.strftime(start_date, "%Y-%m-%d")


def get_end_date_begin_of_week():
    return dt.now().strftime("%Y-%m-%d")


def get_utc_now_date_string(config):
    return dt.now().strftime(config.date_time_format)


def get_run_reg_to_temp_balance_report_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result(
                    'get_reg_to_temp_balance_report_details')['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }



def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_reg_to_temp_balance_us_data_row(items,time_zone):

    personnelnumber = items['Employee_ID']

    timeoff_balance = items['Time_Off_Balance']

    return ["P2010",
            personnelnumber,
            "US",
            "000",
            "INS",
            "2010",
            "2801",
            dt.strptime(pendulum.now(time_zone).strftime("%d-%m-%Y"), "%d-%m-%Y").strftime(
                "%Y%m%d"),
            dt.strptime(pendulum.now(time_zone).strftime("%d-%m-%Y"), "%d-%m-%Y").strftime(
                "%Y%m%d"),
            "",
            "",
            "000",
            "",
            "2801",
            "",
            "",
            "",
            "",
            "USD",
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
        'exported': item['Term Exported']
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


def reg_to_temp_gsap_child_conf(config):

    return {
        'location': 'USA',
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"
                                                                          ), "displayText", "USA", "uri")),
        'dag_start_date_time': get_utc_now_date_string(config),
        'reg_to_temp_balance_report_name': config.reg_to_temp_balance_report_name,
        'file_name': config.file_name_prefix + "_" + dt.now().strftime("%Y%m%d%H%M%S") + "_USTIME_HRMD"+get_sequence(config.USA_PAYROLL_CALENDAR, config.time_zone)+"_DUT8G2I",
        'encyrpt_file': config.encrypt_output_file
    }


def get_sequence(calendra_mapper, time_zone):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    return "06" if bool(rail.find_first_by_attr_and_get_attr(
        calendra_mapper, "payroll_processing_date", current_date)) else "05"
