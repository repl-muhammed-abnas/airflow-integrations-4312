"""
Custom utility methods for Unisys Fieldglass time export integration
Only contains functions that are actually used in the integration

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
from datetime import datetime
from ast import literal_eval
import rail
import json
import pendulum

REPORT_DATE_FORMAT = '%B %d, %Y'
EXPORT_DATE_FORMAT = '%m/%d/%Y'


def get_day(date_string):
    """Get day of week from date string in report format"""
    return datetime.strptime(date_string, REPORT_DATE_FORMAT).strftime('%A')


def week_start_date(date_string):
    """Convert date string from report format to export format"""
    return datetime.strptime(date_string, REPORT_DATE_FORMAT).strftime(EXPORT_DATE_FORMAT)

def get_export_rows(item):
    """
    Generate Fieldglass export rows from task results

    Args:
        task_id: Task ID to get results from

    Returns:
        List of formatted export rows for Fieldglass
    """
    row = {
            "ts_period_uri": item['timesheet_period_uri'],
            "WorkOrder_ID": item['purchase_order_id'],
            "Date": week_start_date(item['timesheet_start_date']),
            "Rate_Category_Code": item['pay_code_name'],
            "Sat_Hrs": 0,
            "Sun_Hrs": 0,
            "Mon_Hrs": 0,
            "Tue_Hrs": 0,
            "Wed_Hrs": 0,
            "Thu_Hrs": 0,
            "Fri_Hrs": 0
    }
    if item.get('entry_date'):
        day = f"{get_day(item['entry_date'])[:3]}_Hrs"
        row[day] = item['total_hours']
    return row


def load_records(log_artifact):
    """Load all records from a log artifact"""
    return rail.load_all_records(log_artifact)


def do_format_logs(dag_run):
    """
    Format and consolidate logs from multiple DAG runs

    Args:
        dag_run: DAG run context containing log artifacts

    Returns:
        List of formatted log records
    """
    log_artifacts = []
    log_records = []

    entrieslogs = dag_run.conf['entrieslogs']
    otherlogs = dag_run.conf['otherlogs']

    if entrieslogs:
        if isinstance(entrieslogs, list):
            log_artifacts.extend(entrieslogs)
        elif isinstance(entrieslogs, str) and entrieslogs[0] == '[':
            entrieslogs = literal_eval(entrieslogs)
            log_artifacts.extend(entrieslogs)
        else:
            log_artifacts.append(entrieslogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)
    
    final_log_records = []

    merge_rows = list(map(lambda x: {
        'merge_rows': f"{x['properties']['ts_period_uri']}|{x['properties']['Rate_Category_Code']}|{x['properties']['WorkOrder_ID']}"
        }, log_records))

    final_data = list({f"{value['merge_rows']}": value for value in merge_rows}.values())

    #pylint: disable=cell-var-from-loop
    for item in final_data:
        entries_log = list(
            filter(lambda x: 
                   (x['properties'].get('ts_period_uri', '') == item['merge_rows'].split('|')[0]) and 
                   (x['properties'].get('Rate_Category_Code', '') == item['merge_rows'].split('|')[1]) and 
                   (x['properties'].get('WorkOrder_ID', '') == item['merge_rows'].split('|')[2]), log_records))
        if len(entries_log) > 0:
            first = entries_log[0]
            final_log_records.append({
                'WorkOrder_ID': first['properties']['WorkOrder_ID'],
                'Date': first['properties']['Date'],
                'Rate_Category_Code': first['properties']['Rate_Category_Code'].upper() if first['properties']['Rate_Category_Code'] else 'REGULAR',
                'Sat_Hrs': round(sum([float(hrs['properties']['Sat_Hrs']) for hrs in entries_log]),2),
                'Sun_Hrs': round(sum([float(hrs['properties']['Sun_Hrs']) for hrs in entries_log]),2),
                'Mon_Hrs': round(sum([float(hrs['properties']['Mon_Hrs']) for hrs in entries_log]),2),
                'Tue_Hrs': round(sum([float(hrs['properties']['Tue_Hrs']) for hrs in entries_log]),2),
                'Wed_Hrs': round(sum([float(hrs['properties']['Wed_Hrs']) for hrs in entries_log]),2),
                'Thu_Hrs': round(sum([float(hrs['properties']['Thu_Hrs']) for hrs in entries_log]),2),
                'Fri_Hrs': round(sum([float(hrs['properties']['Fri_Hrs']) for hrs in entries_log]),2),
            })

    return final_log_records

def get_log_file_name(timezone, prefix):
    prefix = "Log_" + prefix
    current_date = pendulum.now(timezone)
    date_str = current_date.format('MMDDYYYYHHmmss')
    return f"{prefix}_UNISYS_TS_{date_str}.txt"

def get_base_report_filenames(dag_run):
    log_filename = dag_run.conf.get("log_filename", "")

    prefix, ts_part = log_filename.split("_UNISYS_TS_")
    timestamp = ts_part.replace(".csv", "").replace(".pgp", "").replace(".txt", "")

    return {
        "base_report1": f"BaseReport1_{prefix}_UNISYS_TS_{timestamp}.txt",
        "base_report2": f"BaseReport2_{prefix}_UNISYS_TS_{timestamp}.txt",
    }

def _conditional_log(label, value_template):
    """
    Helper function for conditional logging based on data availability

    Args:
        label: Label for the log entry
        value_template: Template for the value

    Returns:
        str: Formatted log entry
    """
    # Always show the values since files are always generated when this task runs
    rendered = rail.render_template(
        f"{label} {value_template}"
    )

    return " ".join(rendered.split())


def build_log_message(dag_run, config):
    """
    Build audit log message with export details

    Args:
        dag_run: DAG run context
        config: Configuration object

    Returns:
        str: JSON string of log messages
    """
    logs = [
        f"File was generated at : {dag_run.conf.get('date_timestamp')}",
        f"Name of the base report used to generate the file : {config.employee_pay_report_name}",
        f"Name of the secondary base report used to generate the file : {config.timesheet_period_report_name}",
        f"Total number of records in the base report : {dag_run.conf.get('total_records')}",
        f"Filters used : Timesheet Approval Status: Approved, Start date: {dag_run.conf['start_date']}, End date: {dag_run.conf['end_date']}, Time Entry Type: Worked Time, Time Off",
        rail.render_template(
            "Total records exported : {{ result('format_logs') | length }}"
        ),
        _conditional_log(
            "Export File SFTP Path :",
            config.export_csv_filepath,
        ),
        _conditional_log(
            "Export File Name :",
            "{{ dag_run.conf.log_filename }}.pgp",
        ),
        _conditional_log(
            "Ops File Path :",
            config.export_csv_to_secondary_filepath,
        ),
        _conditional_log(
            "Ops File Name :",
            "{{ dag_run.conf.log_filename }}.pgp",
        ),
        f"Log File Path : {config.export_logs_csv_filepath}",
        rail.render_template(
            "Log File Name : {{ result('get_log_file_name') }}"
        ),
        f"Base Report File Name : " + rail.render_template("{{ result('get_base_report_filenames').base_report1 }}") + \
            " , " + rail.render_template("{{ result('get_base_report_filenames').base_report2 }}"),
        f"Base Report File Path : {config.export_base_report_filepath}",
    ]

    return json.dumps([{"log": log} for log in logs])
