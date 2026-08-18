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


def get_matched_pay_period_end_date(config):
    """Pay Period End Date whose Payroll Processing Date == today (instance time zone),
    or None when today is not a processing date. Same lookup the Biweekly Payroll
    Export uses in ``get_export_end_date``.
    """
    current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
    return rail.find_first_by_attr_and_get_attr(
        config.CANADA_PAYROLL_CALENDER_MAPPER_TO_USE,
        "payroll_processing_date", current_date, "pay_period_end_date")


def resolve_userenddate_window_clause(dag_run, config, column="userenddate"):
    """Run-time SQL predicate bounding the termination-date (User End Date) export
    window. Upper bound = Pay Period End Date (inclusive, "on or before"); lower bound
    = end - 6 months (lookback unchanged).

    QA can pass ``pay_period_end_date`` (dd-mm-yyyy) via "Trigger DAG w/ config" to
    test any pay period on demand. Otherwise the end date is derived from the payroll
    calendar for the current run; if today is not a Payroll Processing Date (and no
    override is given) the window holds everything (selects nothing), so terminations
    are only released on an actual payroll run.
    """
    if 'pay_period_end_date' in dag_run.conf:
        end_dt = dt.strptime(dag_run.conf['pay_period_end_date'], "%d-%m-%Y")
    else:
        matched = get_matched_pay_period_end_date(config)
        if not matched:
            return "1 = 0"
        end_dt = dt.strptime(matched, "%d-%m-%Y")
    start_date = (end_dt + relativedelta(months=-6)).strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")
    return (f"DATE({column}) > DATE('{start_date}') "
            f"AND DATE({column}) <= DATE('{end_date}')")


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


def terminationbalance_gsap_child_conf(config, dag_run=None):

    child_conf = {
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
    # Forward the QA override (pay_period_end_date, dd-mm-yyyy) from a manual
    # "Trigger DAG w/ config" so the child can bound the window to a chosen pay period.
    if dag_run and 'pay_period_end_date' in dag_run.conf:
        child_conf['pay_period_end_date'] = dag_run.conf['pay_period_end_date']
    return child_conf


def get_sequence(calendra_mapper, time_zone):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    return "03" if bool(rail.find_first_by_attr_and_get_attr(
        calendra_mapper, "payroll_processing_date", current_date)) else "02"
