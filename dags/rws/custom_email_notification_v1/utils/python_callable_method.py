from datetime import datetime, timezone, timedelta
import ast
import rail
from airflow.models import DagRun
null = None

def get_report_uri(reportname):
    reports = rail.result('get_all_reports')
    for report in reports:
        if report.get('displayText') == reportname:
            return report.get('uri')
    return null

def get_date_in_format():
    return (datetime.now(timezone.utc)-timedelta(days=1)).strftime("%b %-d, %Y")

def is_username_not_present():
    rows = rail.load_all_records(rail.result('query_timesheet_data_for_end_date'))
    if rows and rows[0] and rows[0]['username']:
        return False
    return True

def get_timezone_hours_difference(timezones,iananame):
    hours = None
    for tz in timezones:
        if tz['timezonename'] == iananame:
            hours = tz['hours']
    return hours

def get_dag_runs(dag_id):
    dag_runs = []
    for run in DagRun.find(dag_id=dag_id, execution_start_date=datetime.now(timezone.utc)-timedelta(minutes=60)):
        dag_runs.append({
            'id': run.id,
            'run_id': run.run_id,
            'state': run.state,
            'dag_id': run.dag_id,
            'execution_date': run.execution_date.isoformat(),
            'conf': run.conf
        })

    return dag_runs

def get_items_to_add(dag_run):
    timesheets= rail.load_all_records(rail.result('query_timesheets_waiting_on_approver'))
    timesheet_uris = []
    for timesheet in timesheets:
        timesheet_uris.append(timesheet['timesheeturi'])
    approver = dag_run.conf['approver']
    approver_uri= dag_run.conf['approveruri']
    time= rail.get_dag_run_var(dag_run.conf['timezonevariablename'])
    items = [ {
        'timesheeturi': uri,
        'approvername': approver,
        'approveruri': approver_uri,
        'time': time
    } for uri in timesheet_uris ]
    return items

def check_mail_status_for_approver(dag_run):
    jobs = ast.literal_eval(dag_run.conf['previousjobs'])
    if jobs:
        for job in jobs:
            if job['conf']['approveruri'] == dag_run.conf['approveruri'] and job['state'] != 'failed':
                return True
    return False



def get_iananame():
    timesheets= rail.load_all_records(rail.result('query_timesheets_waiting_on_approver'))
    return timesheets[0]['iananame'] if timesheets else ''

def get_approver_data():
    user_list = rail.load_all_records(rail.result('create_userlist_collection'))
    approver = rail.result('for_each_approver').strip()
    return {
        "timesheetperiod": rail.result('foreach_item_in_csv_do')['Timesheet Period'],
        "user": rail.result('foreach_item_in_csv_do')['User Name'],
        "timesheeturi": rail.result('foreach_item_in_csv_do')['timesheet uri'],
        "status": rail.result('foreach_item_in_csv_do')['Approval Status'],
        "approver": rail.result('for_each_approver').strip(),
        "useruri": rail.result('foreach_item_in_csv_do')['user uri'],
        "approveruri": rail.find_first_by_attr_and_get_attr(user_list, 'username', approver, 'useruri'),
        "iananame": rail.find_first_by_attr_and_get_attr(user_list, 'username', approver, 'timezone')
    }
