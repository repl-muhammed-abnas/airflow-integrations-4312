from datetime import datetime
from pendulum import now
import rail

null = None

SQL_DATEFORMAT = "%Y-%m-%d"
REP_DATE_FORMAT = "%m/%d/%Y"


def get_date(entry_date, date_format):

    try:
        if entry_date and datetime.strptime(entry_date, date_format):
            return datetime.strftime(datetime.strptime(entry_date, date_format), SQL_DATEFORMAT)
    except ValueError:
        return null
    return null


def change_date_format(date_str, source_format, target_format):
    """Convert date string from source format to target format."""
    dt = datetime.strptime(date_str, source_format)
    return dt.strftime(target_format)


def can_process_run_test(time_zone, schedule_mapper, mapper_date_format):
    """
    Return True when today's date in time_zone matches any mapper's execution_date.
    """
    from pendulum import now
    current_date = now(time_zone).date()
    
    for item in schedule_mapper:
        if datetime.strptime(item.get("execution_date"), mapper_date_format).date() == current_date:
            return True
    return False


def get_todays_mapper_records(time_zone, schedule_mapper, mapper_date_format):
    """
    Get all mapper records that match today's date.
    """
    from pendulum import now
    current_date = now(time_zone).date()
    
    return [
        item for item in schedule_mapper
        if datetime.strptime(item.get("execution_date"), mapper_date_format).date() == current_date
    ]


def filter_mapper_records_with_org_uri(mapper_records, org_structures):
    """
    Match mapper records with org structure URIs based on org_code.
    """
    org_code_to_uri = {org['code']: org['uri'] for org in org_structures}
    
    matched_records = []
    for record in mapper_records:
        org_code = record.get('org_code')
        if org_code in org_code_to_uri:
            matched_record = dict(record)
            matched_record['org_uri'] = org_code_to_uri[org_code]
            matched_records.append(matched_record)
    
    return matched_records

def get_processed_import_records(ENTRY_DATE_FORMAT):
    input_data = rail.load_all_records(rail.result("create_csv_collection"))
    return rail.write_json_artifact([
        {
            **item,
            "task_name": item["full_task_path"].split("|")[-1].strip(),
            "entry_date_sql": get_date(item["entry_date"], ENTRY_DATE_FORMAT)
        } for item in input_data
    ])

def get_validation_error_message(item):
    missing_fields = []
    required_fields = {
        'ID': 'unique_id',
        'Login': 'employee_id',
        'FECHA_WORK': 'entry_date',
        'HORAS': 'hours',
        'PEP': 'project_id',
        'TAREA': 'full_task_path'
    }

    for field_name, field_key in required_fields.items():
        if not item[field_key]:
            missing_fields.append(f"{field_name} value is missing")
            continue
        if field_key == "hours" and float(item[field_key]) < 0:
            missing_fields.append(f"{field_name} are not valid")
        if field_key == "entry_date" and item["entry_date"] and not item["entry_date_sql"]:
            missing_fields.append("Entry date is not valid, the expected format is DD/MM/YYYY")

    return "; ".join(missing_fields)

def get_email_log_details(log_file_path, dag_run, STANDARD_EMAIL_DATE_FORMAT):
    current_time = now()
    start_time_str = dag_run.conf['start_time']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_file_name": dag_run.conf['log_filename'],
        "log_filepath": log_file_path,
        "input_filename": dag_run.conf['source_filename'],
        "total_record_count": dag_run.conf['total_record_count']
    }

def get_submitted_timesheet_uris():

    timesheet_data = rail.result('get_timesheet_details')
    submitted_uris = set()

    for timesheet in timesheet_data:
        if timesheet.get('timesheet_status') != 'Not Submitted':
            timesheet_uri = timesheet.get('timesheet_uri')
            if timesheet_uri:
                submitted_uris.add(timesheet_uri)

    return list(map(lambda ts: {'ts_uri': ts}, list(submitted_uris)))


def filter_timesheets_for_reopen():
  
    timesheet_details = rail.result('get_timesheet_details') or []
    reopen_list = []
    submit_for_approval_list = []
    force_approve_list = []
    seen_reopen = set()
    seen_submit = set()
    seen_force = set()

    for timesheet in timesheet_details:
        timesheet_uri = timesheet.get('timesheet_uri') or timesheet.get('timesheetUri')
        timesheet_status = timesheet.get('timesheet_status') or timesheet.get('timesheetStatus')
        if not timesheet_uri or not timesheet_status:
            continue

        if timesheet_status in ('Waiting for Approval', 'Approved') and timesheet_uri not in seen_reopen:
            seen_reopen.add(timesheet_uri)
            reopen_list.append({'timesheet_uri': timesheet_uri, 'timesheet_status': timesheet_status})

        if timesheet_status == 'Waiting for Approval' and timesheet_uri not in seen_submit:
            seen_submit.add(timesheet_uri)
            submit_for_approval_list.append({'timesheet_uri': timesheet_uri, 'timesheet_status': timesheet_status})

        if timesheet_status == 'Approved' and timesheet_uri not in seen_force:
            seen_force.add(timesheet_uri)
            force_approve_list.append({'timesheet_uri': timesheet_uri, 'timesheet_status': timesheet_status})

    return {
        'reopen': reopen_list,
        'submit_for_approval': submit_for_approval_list,
        'force_approve': force_approve_list
    }


def do_format_logs(dag_run):
    """Format logs for output"""
    formatted_logs = []
    timeentrylogs = dag_run.conf.get('timeentrylogs') or []
    otherlogs = dag_run.conf.get('otherlogs')
    
    logs = timeentrylogs + ([otherlogs] if otherlogs else [])

    if logs:
        for timeentry in logs:
            log_records = rail.load_all_records(timeentry)
            for log in log_records:
                if isinstance(log, dict) and 'properties' in log:
                    formatted_logs.append(
                        {**log['properties'], "ecid": log.get("ecid", "")})

    success_count = len(
        list(filter(lambda log: log.get('status') == "Success", formatted_logs)))
    error_count = len(
        list(filter(lambda log: log.get('status') == "Error", formatted_logs)))
    exception_count = len(
        list(filter(lambda log: log.get('status') == "Exception", formatted_logs)))

    return {
        'logs': rail.write_json_artifact(formatted_logs),
        'success_count': success_count,
        'error_count': error_count,
        'exception_count': exception_count,
        'total_count': len(formatted_logs)
    }

def filter_collection(collection_name: str, predicate, result_collection: str):

    rows = rail.get_collection(collection_name) or []
    out = [r for r in rows if predicate(r)]
    rail.create_collection(name=result_collection, rows=out)
    return out

def unique_values(source_collection: str, column: str, result_key: str = None):

    rows = rail.get_collection(source_collection) or []
    uniq = list({str(r.get(column)).strip() for r in rows if r.get(column)})
    uniq.sort()
    return uniq

def unique_timesheet_start_dates(source_collection: str = "usertimeentryrecords"):

    rows = rail.get_collection(source_collection) or []
    uniq = sorted({r.get('timesheet_start_date') for r in rows if r.get('timesheet_start_date')})
    return [{"timesheet_start_date": d} for d in uniq]


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default

def is_guessing_hours_row(row: dict) -> bool:
    task = str(row.get('task name', row.get('taskname', ''))).strip().lower()
    hrs  = safe_float(row.get('hours', 0), default=0.0)
    return (task == 'guessing hours') and (hrs > 0.0)

def lowercased_columns(headers: list[str]) -> dict:
    return {h: h.lower() for h in headers}

def map_entry_to_revision_group_uri(timesheet_revision_groups_result, entry_row):
    if not timesheet_revision_groups_result or not entry_row:
        return None

    groups = timesheet_revision_groups_result.get('timeEntryRevisionGroups') or []
    target_date = entry_row.get('entry_date')
    target_user = entry_row.get('user_uri') or entry_row.get('useruri') or entry_row.get('user')

    for g in groups:
        group_uri = g.get('uri') or g.get('timeEntryRevisionGroupUri') or (g.get('timeEntryRevisionGroup') or {}).get('uri')
        ed = g.get('entryDate') or (g.get('timeEntryRevisionGroup') or {}).get('entryDate')
        entry_date = None
        if ed:
            y = ed.get('year'); m = ed.get('month'); d = ed.get('day')
            if y and m and d:
                entry_date = f"{y:04d}-{m:02d}-{d:02d}"
        user_uri = (g.get('user') or {}).get('uri') or (g.get('timeEntryRevisionGroup') or {}).get('user', {}).get('uri')

        if target_date and entry_date and str(target_date).startswith(entry_date) and target_user and user_uri and target_user == user_uri:
            return group_uri
    return None


def build_zero_hours_groups_from_get_response(get_response):

    if not get_response:
        return []

    groups = []
    time_groups = get_response.get('timeEntryRevisionGroups') or get_response.get('TimeEntryRevisionGroups') or []
    for grp in time_groups:
        new_grp = dict(grp)
        new_revs = []
        for rev in grp.get('timeEntryRevisions', []):
            new_rev = dict(rev)
            task_name = str(new_rev.get('taskName') or new_rev.get('task_name') or '').strip().lower()
            if task_name == 'guessing hours':
                if 'hours' in new_rev:
                    new_rev['hours'] = 0
                interval = new_rev.get('interval')
                if isinstance(interval, dict):
                    new_rev['interval'] = {
                        "hours": 0,
                        "minutes": 0,
                        "seconds": 0,
                        "milliseconds": 0,
                        "microseconds": 0
                    }
            new_revs.append(new_rev)
        new_grp['timeEntryRevisions'] = new_revs
        groups.append(new_grp)
    return groups
