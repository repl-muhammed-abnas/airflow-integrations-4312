import json
from datetime import date, timedelta

import rail


def get_filter_uri(report_details):
    enabled_filters = report_details['filterConfiguration']['enabledFilters']
    return {
        "entry_date_filter": rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'EntryDateFilter', 'uri'),
        "user_filter_uri": rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'UserFilter', 'uri'),
    }


def create_report_filter(dag_run, report_filter_uri):
    timesheet_start_and_end_date = dag_run.conf["timesheet_period"].split("-")
    report_filter_list = [
        {
            "reportFilterUri": report_filter_uri.get("user_filter_uri"),
            "value": dag_run.conf["user_uri"].split(":")[-1].strip(),
        },
        {
            "reportFilterUri": report_filter_uri.get("entry_date_filter"),
            "value": None
        },
        {
            "reportFilterUri": report_filter_uri.get("entry_date_filter"),
            "value": timesheet_start_and_end_date[0]
        },
        {
            "reportFilterUri": report_filter_uri.get("entry_date_filter"),
            "value": timesheet_start_and_end_date[1]
        },
    ]
    return json.dumps(report_filter_list)


def get_timesheet_data(item):
    return [
        item.get("Timesheet Period"),
        item.get("User Name"),
        item.get("Pay Code Name"),
        item.get("Pay Code Code"),
        item.get("Entry Date"),
        item.get("Pay Code Hours"),
        item.get("User URI"),
        item.get("timesheet URI"),
        item.get("Approval Status"),
        item.get("User First Name"),
        item.get("User Last Name"),
        item.get("Location (Current)")
    ]


def get_qbo_employee_id():
    response = rail.result('lookup_qbo_employee')
    employees = response.get('QueryResponse', {}).get('Employee', [])
    return employees[0].get('Id') if employees else None


# multiplying factor of 0.6 is as per workato logic. In the report,
# there are two digits to the right of the decimal point
def parse_hours_minutes(dag_run):
    paycodehours = dag_run.conf.get('pay_code_hours')
    hours = int(paycodehours.split('.')[0].strip()) if '.' in paycodehours else paycodehours
    minutes = round(int(paycodehours.split('.')[1].strip()) * 0.6) if '.' in paycodehours else 0
    return {'hours': hours, 'minutes': minutes}


def format_synced_hours(hours_minutes):
    return f"{hours_minutes['hours']}.{hours_minutes['minutes']:02d}"


def determine_effective_pay_type(dag_run):
    pay_code_code = dag_run.conf.get('pay_code_code', '')
    location = dag_run.conf.get('location', '')
    if location and "Salary" in location:
        if "Regular" in pay_code_code or "Overtime" in pay_code_code:
            return "Salary"
    return pay_code_code


def find_pay_item_id(config):
    employee_id = str(rail.result('get_qbo_employee_id'))
    effective_pay_type = rail.result('determine_effective_pay_type')
    for entry in config.WCS_PAY_ITEM_REFERENCE_MAPPER:
        if entry.get('Employee ID') == employee_id and entry.get('Pay Type') == effective_pay_type:
            return entry.get('Pay Item ID')
    return None


def build_pipe_value(dag_run):
    hours_minutes = rail.result('calculate_hours_minutes')
    synced_hours = (str(hours_minutes.get('hours', '')) + "." + str(hours_minutes.get('minutes', ''))) if hours_minutes else "."
    return (
        dag_run.conf.get('user_name', '') + "|"
        + dag_run.conf.get('timesheet_period', '') + "|"
        + dag_run.conf.get('entry_date', '').replace('/', '-') + "|"
        + dag_run.conf.get('pay_code_hours', '') + "|"
        + synced_hours + "|"
        + (rail.result('determine_effective_pay_type') or '')
    )


def build_skipped_pipe_value(dag_run):
    hours_minutes = rail.result('calculate_hours_minutes') or {}
    return (
        dag_run.conf.get('user_name', '') + "|"
        + dag_run.conf.get('timesheet_period', '') + "|"
        + dag_run.conf.get('entry_date', '').replace('/', '-') + "|"
        + dag_run.conf.get('pay_code_hours', '') + "|"
        + str(hours_minutes.get('hours', '')) + "."
        + str(hours_minutes.get('minutes', '')) + "|"
    )


def build_not_found_pipe_value(dag_run):
    return (
        dag_run.conf.get('user_name', '') + "|"
        + dag_run.conf.get('timesheet_period', '') + "||"
        + dag_run.conf.get('pay_code_hours', '') + "|.|"
    )


def is_entry_older_than_90_days(entry):
    entry_date_str = entry.get('properties', {}).get('date')
    if not entry_date_str:
        return False
    
    cutoff_date = date.today() - timedelta(days=90)
    return date.fromisoformat(entry_date_str) <= cutoff_date

