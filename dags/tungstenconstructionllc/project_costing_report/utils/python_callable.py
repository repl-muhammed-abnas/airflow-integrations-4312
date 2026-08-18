# pylint: disable=too-many-statements line-too-long
from datetime import datetime
from rail import find_first_by_attr_and_get_attr, result, load_all_records, get_current_context
import json
import logging


logger = logging.getLogger(__name__)


def print_webhook_data(op_args):
    print(f"print_webhook_data op_args: {op_args}")


def get_year_month_date(date_str):
    date = datetime.strptime(date_str, '%m%d%Y')
    return {
            "year": date.year,
            "month": date.month,
            "day": date.day
            }

def round_half_up(value, decimals=2):
    multiplier = 10 ** decimals
    return int(value * multiplier + 0.5) / multiplier

def get_start_end_date_difference(date_range):
    return int((datetime.strptime(date_range.split('-')[1], '%m%d%Y') - datetime.strptime(date_range.split('-')[0], '%m%d%Y')).days)

def get_request_body_payroll_download_batch(dag_run):
    tenant = result('get_file_format_script_uri').split(':')[2]
    return {
        "columnUris": [
            "urn:replicon:pay-run-column:user-login-name",
            "urn:replicon:pay-run-column:timesheet-period",
            f"urn:replicon-tenant:{tenant}:pay-run-payroll-item-metadata-object-column:36ba887d-28a3-4736-a131-4d663b696aec:name",
            f"urn:replicon-tenant:{tenant}:pay-run-pay-code-hours-column:4",
            f"urn:replicon-tenant:{tenant}:pay-run-pay-code-hours-column:1"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "dateRange": {
                        "startDate": get_year_month_date(dag_run.conf['webhook']['data']['dateRange'].split('-')[0]),
                        "endDate": get_year_month_date(dag_run.conf['webhook']['data']['dateRange'].split('-')[1])
                    }
                }
            }
        },
        "fileFormatScriptUri": result('get_file_format_script_uri')
    }

def get_request_body_execute_in_background():
    return {
        "batchUri": result('create_payroll_download_batch')
    }

def check_if_daterange_does_not_contains_null(dag_run):
    daterange = (dag_run.conf['webhook']['data'].get('dateRange', '')).split('-')
    return bool((len(daterange) > 1 and daterange[0] and daterange[1]) and (daterange[0] != 'null' or daterange[1] != 'null'))


def get_entry_date_filter_and_project_filter_uri():
    enable_filter = result('hourly_cost_report_for_project_costing')['filterConfiguration']['enabledFilters']
    return {
        "EntryDateFilter":find_first_by_attr_and_get_attr(enable_filter, 'displayText', 'EntryDateFilter', 'uri'),
        "ProjectFilter":find_first_by_attr_and_get_attr(enable_filter, 'displayText', 'ProjectFilter', 'uri')
    }

def create_report_filter_1_list(dag_run):
    projects = dag_run.conf['webhook']['data'].get('projectIds', '').split(',')
    date = (dag_run.conf['webhook']['data'].get('dateRange', '')).split('-')
    report_filter_1_list = []
    for project in projects:
        report_filter_1_list.append({
            "reportFilterUri": result('get_entry_date_filter_and_project_filter_uri').get('ProjectFilter'),
            "value": project
        })
    report_filter_1_list.append({
            "reportFilterUri": result('get_entry_date_filter_and_project_filter_uri').get('EntryDateFilter'),
            "value": None
        })
    report_filter_1_list.append({
            "reportFilterUri": result('get_entry_date_filter_and_project_filter_uri').get('EntryDateFilter'),
            "value": datetime.strptime(date[0], '%m%d%Y').strftime("%m/%d/%Y")
        })
    report_filter_1_list.append({
            "reportFilterUri": result('get_entry_date_filter_and_project_filter_uri').get('EntryDateFilter'),
            "value": datetime.strptime(date[1], '%m%d%Y').strftime("%m/%d/%Y")
        })
    return json.dumps(report_filter_1_list)

def get_date_range_project_expense_type_filter_uri():
    enabled_filters = result('expense_report_for_project_costing')['filterConfiguration']['enabledFilters']
    return {
        "DateRangeFilter_IncurredDate":find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'DateRangeFilter_IncurredDate', 'uri'),
        "ProjectFilter":find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'ProjectFilter', 'uri'),
        "ExpenseTypeFilter":find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'ExpenseTypeFilter', 'uri')
    }

def get_per_diem_expense_code_id():
    return find_first_by_attr_and_get_attr(result('get_all_expense_codes'),'name','Per Diem','uri').split(":")[-1]

def add_value_to_filter_1_list(expense_type_filter_uri, expense_code_id):
    filter_1_list = json.loads(result('create_report_filter_1_list'))
    filter_1_list.append({
            "reportFilterUri": expense_type_filter_uri,
            "value": expense_code_id
        })
    return json.dumps(filter_1_list)


def add_values_report_filter_2_list(dag_run):
    projects = dag_run.conf['webhook']['data'].get('projectIds', '').split(',')
    date = (dag_run.conf['webhook']['data'].get('dateRange', '')).split('-')
    report_filter_2_list = []
    for project in projects:
        report_filter_2_list.append({
            "reportFilterUri": result('get_date_range_project_expense_type_filter_uri').get('ProjectFilter'),
            "value": project
        })
    report_filter_2_list.append({
            "reportFilterUri": result('get_date_range_project_expense_type_filter_uri').get('DateRangeFilter_IncurredDate'),
            "value": None
        })
    report_filter_2_list.append({
            "reportFilterUri": result('get_date_range_project_expense_type_filter_uri').get('DateRangeFilter_IncurredDate'),
            "value": datetime.strptime(date[0], '%m%d%Y').strftime("%m/%d/%Y")
        })
    report_filter_2_list.append({
            "reportFilterUri": result('get_date_range_project_expense_type_filter_uri').get('DateRangeFilter_IncurredDate'),
            "value": datetime.strptime(date[1], '%m%d%Y').strftime("%m/%d/%Y")
        })
    return json.dumps(report_filter_2_list)

def get_converted_hourly_cost_data(item):
    if not item:
        return []
    return {
        'client': item['client'] if item['client'] else None,
        'hourlycostamount': item['hourlycostamount'] if item['hourlycostamount'] else None,
        'equipmentcost': item['equipmentcost'] if item['equipmentcost'] else None
    }

def get_converted_expense_data(item):
    if not item:
        return []
    return {
        'amount': item['amount'] if item['amount'] else None
    }

def get_gross_income(dag_run):
    hourlycostamount = float(load_all_records(result('query_hourly_cost_data'))[0].get('hourlycostamount'))
    if not hourlycostamount:
        return 0
    return float(hourlycostamount * float(dag_run.conf['rthours'])) + float(hourlycostamount * 1.5 * float(dag_run.conf['othours'])) + \
    round_half_up(float((float(dag_run.conf['rthours']) + float(dag_run.conf['othours'])) * float(load_all_records(result('query_hourly_cost_data'))[0].get('equipmentcost'))), 2)

def get_log_success_properties(dag_run):
    return {
                "jobid": dag_run.conf["jobid"],
                "client": load_all_records(result('query_hourly_cost_data'))[0].get('client'),
                "project": dag_run.conf['projectname'],
                "timesheetperiod": datetime.strptime(dag_run.conf['timsheetstart'], '%Y-%m-%d').strftime("%Y-%m-%d") + " - " + datetime.strptime(dag_run.conf['timesheetend'], '%Y-%m-%d').strftime("%Y-%m-%d"),
                "project_RT_hours": dag_run.conf['rthours'],
                "project_OT_hours": dag_run.conf['othours'],
                "hourlycost": load_all_records(result('query_hourly_cost_data'))[0].get('hourlycostamount'),
                "equipmentcost": load_all_records(result('query_hourly_cost_data'))[0].get('equipmentcost'),
                "gross income": get_gross_income(dag_run),
                "perdiem_amount": get_perdiem_amount_for_lookup_table()
            }

def get_values_from_lookup_table(item, project_costing_lookup_table, jobid):
    aggregated_data = {}
    timesheetperiod = item['timsheetstart'] + " - " + item['timesheetend']
    matched = [
        entry for entry in project_costing_lookup_table
        if entry['properties'].get('project') == item.get('projectname')
        and entry['properties'].get('timesheetperiod') == timesheetperiod
    ]

    if not matched:
        return {
            "jobid": "final_" + jobid,
            "client": "",
            "project": item.get('projectname'),
            "daterange": timesheetperiod,
            "perdiemtotals": 0.00,
            "grossincometotals": 0.00,
            "weeklytotals": 0.00
        }
    aggregated_data["jobid"] = "final_" + jobid
    aggregated_data["client"] = matched[0]["properties"].get("client") if matched[0]["properties"].get("client") else ""
    aggregated_data["project"] = item.get('projectname')
    aggregated_data["daterange"] = timesheetperiod
    aggregated_data["perdiemtotals"] = round_half_up(float(sum(match['properties'].get('perdiem_amount', 0) for match in matched)), 2)
    aggregated_data["grossincometotals"] = round_half_up(float(sum(match['properties'].get('gross income', 0) for match in matched)), 2)
    aggregated_data["weeklytotals"] = round_half_up(float(aggregated_data["perdiemtotals"] + aggregated_data["grossincometotals"]), 2)
    return aggregated_data



def accumulate_items_to_data_per_project_and_timesheet_list(jobid):
    distinct_projects_and_timesheets = result('get_distinct_projects_and_timesheets_data')
    project_costing_lookup_table = load_all_records(result('project_costing_generate_report_lookup_table'))
    response = []
    for item in distinct_projects_and_timesheets:
        aggregated_values = get_values_from_lookup_table(item, project_costing_lookup_table, jobid)
        response.append(aggregated_values)
    return response

def get_timesheetperiod(item):
    date = item.get('daterange').split(" - ")
    return datetime.strptime(date[0].strip(), '%Y-%m-%d').strftime("%m/%d") + " - " + datetime.strptime(date[1].strip(), '%Y-%m-%d').strftime("%m/%d")

def sum_field(accumulate_items, project, field):
    total = 0.0

    for accumulate_item in accumulate_items:
        if accumulate_item.get("project") == project:
            value = accumulate_item.get(field)
            total += float(value) if value else 0.0

    return round_half_up(total, 2)


def get_summarize_final_data(project, config):
    accumulate_items = result('accumulate_items_to_data_per_project_and_timesheet_list')

    return {
        "Client": "Weekly Total",
        "Project": None,
        "Daterange": None,
        "Perdiemtotals": f"{config.CURRENCY_PREFIX}{sum_field(accumulate_items, project, 'perdiemtotals'):.2f}",
        "Grossincometotals": f"{config.CURRENCY_PREFIX}{sum_field(accumulate_items, project, 'grossincometotals'):.2f}",
        "Weeklytotals": f"{config.CURRENCY_PREFIX}{sum_field(accumulate_items, project, 'weeklytotals'):.2f}",
    }


def get_perdiem_amount_for_lookup_table():
    query_per_diem_expense_data = result('query_per_diem_expense_data')
    if not query_per_diem_expense_data:
        logger.info("query_per_diem_expense_data is not available, defaulting perdiem_amount to 0.00")
        return 0.00
    per_diem_expense_data = load_all_records(query_per_diem_expense_data)
    if not per_diem_expense_data:
        return 0.00
    sum_amount = per_diem_expense_data[0].get('total_amount')
    return float(sum_amount) if sum_amount else 0.00


def if_first_is_present_and_last_is_not():
    indx = get_current_context()['ti'].xcom_pull(task_ids="iterate_distinct_project_list", key="index") + 1
    return bool(indx == 1 and len(load_all_records(result('distinct_projects_in_lookup_table'))) != 1)

def if_first_and_last_is_not_present():
    indx = get_current_context()['ti'].xcom_pull(task_ids="iterate_distinct_project_list", key="index") + 1
    return bool(indx != 1 and len(load_all_records(result('distinct_projects_in_lookup_table'))) != indx)

def if_last_is_present_first_is_not():
    indx = get_current_context()['ti'].xcom_pull(task_ids="iterate_distinct_project_list", key="index") + 1
    return bool(indx != 1 and len(load_all_records(result('distinct_projects_in_lookup_table'))) == indx)

def if_first_and_last_is_present():
    indx = get_current_context()['ti'].xcom_pull(task_ids="iterate_distinct_project_list", key="index") + 1
    return bool(indx == 1 and len(load_all_records(result('distinct_projects_in_lookup_table'))) == 1)
