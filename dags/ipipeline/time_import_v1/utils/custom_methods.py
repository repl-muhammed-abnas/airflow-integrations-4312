from datetime import timedelta, datetime
from pendulum import now
from airflow.models import Variable
import json
import rail
import itertools


def get_required_timestamps(config):
    _now = now(config.tempo_time_zone)
    tempo_lookback_timestamp = Variable.get(
        config.tempo_time_entries_lookback_date, default_var=(_now - timedelta(hours=6)).strftime(config.TEMPO_DATE_FORMAT))
    return {
        'tempo_lookback_timestamp': tempo_lookback_timestamp,
        'current_timestamp': _now.strftime(config.TEMPO_DATE_FORMAT)
    }


def transform_tempo_api_response():
    api_response_json_load = json.loads(rail.result(
        'extract_tempo_time_entries')) if rail.result('extract_tempo_time_entries') else {}
    results_json = api_response_json_load.get('results', [])
    time_entry_list = [{
        'task_jira_issue_id': record.get('issue', {}).get('id'),
        'hours': float(record.get('timeSpentSeconds')/3600) if record.get('timeSpentSeconds') else 0,
        'time_entry_date': record.get('startDate'),
        'time_entry_start_time': record.get('startTime'),
        'time_entry_comment': record.get('description'),
        'author_jira_account_id': record.get('author', {}).get('accountId'),
        'updated_at': record.get('updatedAt')
    } for record in results_json] if results_json else []

    return time_entry_list


def get_user_email(user_details_from_jira):
    details = json.loads(user_details_from_jira) if user_details_from_jira else {}

    if details.get('errorMessages'):
        return " ; ".join(details.get('errorMessages'))

    return details.get('emailAddress', '')


def transform_task_jira_api_result(task_jira_api_res):
    task_api_response = json.loads(task_jira_api_res) if task_jira_api_res else {}
    fields = task_api_response.get('fields', {})
    task_details = {
        'task_issue_id': task_api_response.get('id', ''),
        'task_jira_id': task_api_response.get('key', ''),
        'task_issuetype': fields.get('issuetype', {}).get('name', '') if fields.get('issuetype') else '',
        'task_summary': fields.get('summary', ''),
        # Story
        'task_parent_issue_id': fields.get('parent').get('id', '') if fields.get('parent') else '',
        'task_parent_jira_id': fields.get('parent').get('key', '') if fields.get('parent') else '',
        'task_parent_jira_summary': (
            fields.get('parent').get('fields').get('summary', '') if fields.get('parent').get('fields') else '') if fields.get('parent') else '',
        'task_type': fields.get('customfield_16456').get('value', '') if fields.get('customfield_16456') else (
            fields.get('customfield_11301').get('value', '') if fields.get('customfield_11301') else '')
    } if fields else {}

    return task_details


def get_epic_issue_id(story_issue_details):
    story_details = json.loads(story_issue_details) if story_issue_details else {}
    if story_details.get('fields'):
        if story_details['fields'].get('parent'):
            return story_details['fields']['parent'].get('id', '')
    return ''


def get_project_code_from_epic_level():
    epic_data = json.loads(rail.result("get_epic_details")) if rail.result(
        "get_epic_details") else {}
    replicon_id = epic_data['fields'].get('customfield_16301', '') if epic_data.get('fields') else ''
    return replicon_id


def get_process_task_metadata_retrieval(parallel_count):
    dag_runs = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'trigger_jira_metadata_retrieval_{x+1}') if rail.result(
            f'trigger_jira_metadata_retrieval_{x+1}') else []), range(parallel_count)))))

    return dag_runs


def get_process_user_time_entries_dag_ids(parallel_count):
    dag_runs = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'trigger_time_entries_creation_group_by_user_{x+1}') if rail.result(
            f'trigger_time_entries_creation_group_by_user_{x+1}') else []), range(parallel_count)))))

    return dag_runs


def format_task_metadata_logs(task_metadata_retrieval_logs):
    log_artifacts = []
    log_records = []

    logs = task_metadata_retrieval_logs

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    return final_log_records


def page_handler(request, result):
    """Handle pagination for Replicon API responses."""
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_value(data, index, pluck_key):
    """Extract value from Replicon API response cell structure."""
    return data['cells'][index].get(pluck_key, '')


def filter_project_data(result):
    """Transform Replicon project list response into simplified dictionary format."""
    rows_list = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))

    if not rows_list:
        return []

    return list(map(lambda item: {
        'project_name': get_value(item, 0, "textValue"),
        'project_code': get_value(item, 1, "textValue"),
        'project_uri': get_value(item, 0, "uri"),
        'project_status': get_value(item, 2, "textValue"), }, rows_list)) if rows_list else []


def get_validation_details(item, config):

    exceptions = []

    for k, v in config.MANDATORY_FIELDS.items():
        if not (item[k]):
            exceptions.append(v)

    return ("Time entry not processed as - " + " , ".join(exceptions) + " is missing") if exceptions else ''


def user_details_from_replicon(res):
    if not res:
        return {
            'process_further': False,
            'message': "User not found in replicon"
        }
    if str(res[0]['userDetails']['isEnabled']).lower() == 'false':
        return {
            'process_further': False,
            'message': "User is disabled in Replicon"
        }
    if not (res[0]["timesheetTemplate"]) or not (res[0]["timesheetTemplate"]["uri"]):
        return {
            'process_further': False,
            'message': "User doesnt have timesheet template assigned in replicon"
        }

    return {
        'process_further': True,
        'employee_id': res[0]['userDetails']['employeeId'],
        'uri':  res[0]['userDetails']['uri'],
        'timesheet_template_uri': res[0]["timesheetTemplate"]["uri"]
    }


def get_project_validaiton_check_and_details(action):
    if not (rail.result('get_all_project_details')):
        return (False if action == 'check' else "Project not found in Replicon")

    if rail.result(
            'get_all_project_details')['projectDetails']['status']['displayText'] != 'In Progress':
        return (False if action == 'check' else "Project not in Execution Status in Replicon")

    return (True if action == 'check' else '')


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    master_log = dag_run.conf['master_log']
    child_logs = dag_run.conf['child_logs']

    if master_log:
        if isinstance(master_log, list):
            log_artifacts.extend(master_log)
        else:
            log_artifacts.append(master_log)

    if child_logs:
        if isinstance(child_logs, list):
            log_artifacts.extend(child_logs)
        else:
            log_artifacts.append(child_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['total_records'])

    return final_log_records


def get_email_details_callable(dag_run, time_zone):
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"Log_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }

def get_is_billable_status(response):
    billable_entry_allowed_status = ['Billable Only', 'Billable & Non-Billable']
    project_timeandexpenseentry_displaytext = rail.result(
        'get_all_project_details')['projectDetails'].get('timeAndExpenseEntryType').get('displayText', '') if rail.result(
            'get_all_project_details')['projectDetails'].get('timeAndExpenseEntryType') else ''
    task_timeandexpenseentry_displaytext = response.get('timeAndExpenseEntryType').get('displayText', '') if response.get(
        'timeAndExpenseEntryType') else ''
    
    return {
        'project_timeandexpenseentry' : project_timeandexpenseentry_displaytext,
        'task_timeandexpenseentry' : task_timeandexpenseentry_displaytext,
        'is_billable': task_timeandexpenseentry_displaytext in billable_entry_allowed_status
    }