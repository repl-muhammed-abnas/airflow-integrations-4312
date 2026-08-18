from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable, DagRun

def load_artiact_data(data):
    return rail.load_all_records(rail.result(data))

def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):

    current_time = datetime.now(timezone.utc)
    lookup_timestamp_value = Variable.get(
        lookup_log_timestamp_var, default_var=None)

    query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
        current_time - timedelta(hours=lookup_log_timestamp_hours))

    dag_runs = []
    execution_dates = []
    for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
        execution_dates.append(run.execution_date)
        dag_runs.append(run.id)
    if execution_dates:
        max_execution_date = max(execution_dates)
        Variable.set(lookup_log_timestamp_var,
                     (max_execution_date + timedelta(seconds=1)).isoformat())
    return dag_runs

def get_unique_log_artifacts_callable():
    logs = rail.result("get_project_logs")
    project_logs = []
    for log in logs:
        project_logs.extend([log['project_log'], log['exception_log']])
    return list(set(project_logs))

def do_format_logs():
    final_log_records = []
    logs = get_unique_log_artifacts_callable()
    for log in logs:
        final_log_records.extend(rail.load_all_records(log))
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'exception', final_log_records))))
    return rail.write_json_artifact(final_log_records)

mandatory_fields = {
    "project_fields": {
        "projectcode": "projectcode",
    }
}

def get_invalid_logs_property_conf(item):
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields['project_fields']:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "projectcode": item['projectcode'],
        "projectname": item['projectname'],
        'action': 'Validation',
        "details": get_missing_field() + " not present in feed file",
        "Status": 'Exception'
    }

def check_dates_are_valid():
    start_date = rail.result("load_project_data_from_query")['projectstartdate']
    end_date = rail.result("load_project_data_from_query")['projectenddate']
    def is_valid_date(date_str):
        try:
            datetime.strptime(date_str, "%Y%m%d")
            return True
        except (ValueError, TypeError):
            return False

    if not start_date and not end_date:
        return {
            'action': True,
            'message': "Both start and end dates are blank. Allowed."
        }

    if not start_date:
        if is_valid_date(end_date):
            return {
                'action': True,
                'message': "Start date is blank, end date is valid. Allowed."
            }
        else:
            return {
                'action': False,
                'message': "Invalid end date received, expected format is YYYYMMDD"
            }

    if not end_date:
        if is_valid_date(start_date):
            return {
                'action': True,
                'message': "End date is blank, start date is valid. Allowed."
            }
        else:
            return {
                'action': False,
                'message': "Invalid start date received, expected format is YYYYMMDD"
            }

    # Both dates are present
    valid_start = is_valid_date(start_date)
    valid_end = is_valid_date(end_date)

    if not valid_start or not valid_end:
        if not valid_start:
            return {
                'action': False,
                'message': "Invalid start date received, expected format is YYYYMMDD"
            }
        if not valid_end:
            return {
                'action': False,
                'message': "Invalid end date received, expected format is YYYYMMDD"
            }
        

    # Both are valid, now check logical ordering
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")

    if start_dt > end_dt:
        return {
            'action': False,
            'message': "Project start date cannot be after project end date."
        }

    return {
        'action': True,
        'message': "Both dates are valid and logically correct."
    }
