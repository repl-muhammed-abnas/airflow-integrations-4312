from datetime import datetime as dt
import pendulum
import rail
from dateutil.relativedelta import relativedelta


def get_sequence(calendra_mapper, time_zone):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    return "02" if bool(rail.find_first_by_attr_and_get_attr(
        calendra_mapper, "payroll_processing_date", current_date)) else "01"


def get_run_report_payload():
    get_specific_report_details = rail.result('get_report_details')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_start_date_begin_of_week():
    start_date = dt.strptime(dt.utcnow().strftime(
        "%Y-%m-%d"), "%Y-%m-%d") + relativedelta(months=-6)
    return dt.strftime(start_date, "%Y-%m-%d")


def get_end_date_begin_of_week():
    return dt.utcnow().strftime("%Y-%m-%d")


def get_formated_row(item):
    return {
        'employeeid': item['Employee ID'],
        'timeofftypes': item['Time Off Type'],
        'timeoffaccrued': item['Time Off Accrued'],
        'timeofftaken': item['Time Off Taken'],
        'timeoffbalance': item['Time Off Balance'],
        'employeetype': item['Employee Type (Current)'],
        'jobcode': item['Job Code'],
        'location': item['Location (Current)'],
        'paygroup': item['US Pay Group']
    }.values()


def get_final_formated_row(item):
    return {
        'employeeid': item['employeeid'],
        'timeofftypes': item['timeofftypes'],
        'timeoffaccrued': item['timeoffaccrued'],
        'timeofftaken': item['timeofftaken'],
        'timeoffbalance': item['timeoffbalance'],
        'employeetype': item['employeetype'],
        'jobcode': item['jobcode'],
        'location': item['location']
    }.values()


def get_accrued_data_row(item):
    if item['timeofftypes'] == '[USA] Vacation':
        wage_type = '2Q50'
    if item['timeofftypes'] == '[USA] Sick':
        wage_type = '2Q51'
    if item['timeofftypes'] == '[USA] Emergency Leave':
        wage_type = '2Q53'
    if item['timeofftypes'] == '[USA] Floating Holiday':
        wage_type = '2Q52'

    return {
        'employeeid': item['employeeid'],
        'timeofftypes': item['timeofftypes'],
        'balance': item['timeoffaccrued'],
        'balancetype': wage_type

    }.values()


def get_timeofftaken_data_row(item):
    if item['timeofftypes'] == '[USA] Vacation':
        wage_type = '2T50'
    if item['timeofftypes'] == '[USA] Sick':
        wage_type = '2T51'
    if item['timeofftypes'] == '[USA] Emergency Leave':
        wage_type = '2T53'
    if item['timeofftypes'] == '[USA] Floating Holiday':
        wage_type = '2T52'
    return {
        'employeeid': item['employeeid'],
        'timeofftypes': item['timeofftypes'],
        'balance': item['timeofftaken'],
        'balancetype': wage_type

    }.values()


def get_timeoffbalance_data_row(item):
    if item['timeofftypes'] == '[USA] Vacation':
        wage_type = '2R50'
    if item['timeofftypes'] == '[USA] Sick':
        wage_type = '2R51'
    if item['timeofftypes'] == '[USA] Emergency Leave':
        wage_type = '2R53'
    if item['timeofftypes'] == '[USA] Floating Holiday':
        wage_type = '2R52'
    return {
        'employeeid': item['employeeid'],
        'timeofftypes': item['timeofftypes'],
        'balance': item['timeoffbalance'],
        'balancetype': wage_type

    }.values()


def get_balance_us_data_row(items, calendar):

    personnelnumber = items['employeeid']

    timeoff_balance = items['balance']

    end_date = rail.find_first_by_attr_and_get_attr(calendar, "payroll_processing_date", dt.now().strftime("%d-%m-%Y"), "pay_period_end_date") if rail.find_first_by_attr_and_get_attr(
        calendar, "payroll_processing_date", dt.now().strftime("%d-%m-%Y"), "pay_period_end_date") else dt.now().strftime("%d-%m-%Y")

    return ["P2010",
            personnelnumber,
            "US",
            "",
            "INS",
            "2010",
            items['balancetype'],
            dt(dt.now().year, 1, 1).strftime("%Y%m%d"),
            dt.strptime(end_date, "%d-%m-%Y").strftime(
                "%Y%m%d"),
            "",
            "",
            "",
            "",
            items['balancetype'],
            "",
            "",
            "",
            "0.01" if int(float(timeoff_balance)) == 0 else "",
            "USD",
            timeoff_balance,
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


def is_upload_data_to_sftp_failed():
    if get_task_state('upload_export_data_to_sftp') == 'failed':
        return True
    return False


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def is_upload_log_to_sftp_failed():
    if get_task_state('upload_log_data_to_sftp') == 'failed':
        return True
    return False
