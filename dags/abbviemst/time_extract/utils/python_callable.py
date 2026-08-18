"""
Python callable functions for AbbvieMST Time Extract DAG
"""
from airflow.exceptions import AirflowFailException
import rail
from pendulum import now, today


def should_run_based_on_workato_schedule():
    """
    Check if DAG should run based on Workato schedule logic.

    Returns True if current day matches the expected trigger day:
    - 1st is Sunday → run on 3rd
    - 1st is Monday-Thursday → run on 2nd
    - 1st is Friday-Saturday → run on 4th
    """
    current_time = now('US/Central')

    # Mapping: weekday of 1st (Mon=0...Sun=6) → scheduled run day
    scheduled_day = {6: 3, 0: 2, 1: 2, 2: 2, 3: 2, 4: 4, 5: 4}[current_time.start_of('month').weekday()]

    return current_time.day == scheduled_day

def get_required_column_uris():
    task_columns = [item for item in rail.result('get_all_columns') if item["displayText"] == "Task"]
    if task_columns and task_columns[0].get('columns'):
        return {
            "beneficiary_code_uri": rail.find_first_by_attr_and_get_attr(task_columns[0]['columns'], "displayText", "Beneficiary Code", "uri"),
            "compass_root_product_code_uri": rail.find_first_by_attr_and_get_attr(task_columns[0]['columns'], "displayText", "COMPASS Root Project Code", "uri"),
            "compass_uri": rail.find_first_by_attr_and_get_attr(task_columns[0]['columns'], "displayText", "Compass?", "uri"),
            "protocol_code_uri": rail.find_first_by_attr_and_get_attr(task_columns[0]['columns'], "displayText", "Protocol Code", "uri")
        }
    return ""

def get_logging_details():

    current_date = today('US/Central')
    job_start_time = now('US/Central').strftime("%m%d%Y%H%M%S")

    if current_date.month == 1:
        start_date = current_date.subtract(years=1).start_of('year')
    else:
        start_date = current_date.start_of('year')

    end_date = current_date.start_of('month').subtract(days=1)

    return {
        "export_name": "Custom_timeexport_" + job_start_time,
        "file_name": "Replicon_TS_" + job_start_time,
        "job_start_time": job_start_time,
        "start_date": {
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day
        },
        "end_date": {
            "year": end_date.year,
            "month": end_date.month,
            "day": end_date.day
        }
    }

def retrieve_export_uri(response):
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + response)
    return response['timeDataExportUri']

def get_final_extract_data_row(item):
    previous_month = today('UTC').start_of('month').subtract(days=1)
    account_period = previous_month.strftime("%m")

    return [
        item['dept_code'] if item['dept_code'] else "",
        item['employee_id'] if item['employee_id'] else "",
        item['beneficiary_code'] if item['beneficiary_code'] else "",
        item['root_project_code'] if item['root_project_code'] else "",
        item['protocol'] if item['protocol'] else "",
        item['reg_hours'] if item['reg_hours'] else 0,
        "",  # blank column
        account_period,
        item['year'] if item['year'] else "",
    ]