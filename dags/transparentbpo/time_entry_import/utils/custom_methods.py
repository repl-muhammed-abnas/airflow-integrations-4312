from datetime import datetime
from pendulum import now
from ast import literal_eval
from typing import Dict, List, Any
import rail


MANDATORY_FIELDS = ['employee_id', 'work_date', 'start_time', 'end_time', 'project', 'task', 'activity', 'timesheet_category']
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
entry_dateformat = '%m/%d/%Y'
null = None

def validate_entry_date_format():
    valid_record_entry = rail.load_all_records(rail.result("query_valid_records"))
    invalid_records = []
    valid_records = []
    punch_records = {}
    for record in valid_record_entry:
        work_date = record.get('work_date')
        try:
            datetime.strptime(str(work_date).strip(), entry_dateformat)
            datetime.strptime(str(record.get('start_time')).strip(), "%I:%M:%S %p")
            datetime.strptime(str(record.get('end_time')).strip(), "%I:%M:%S %p")
        except Exception:
            invalid_records.append(record)
            continue
        valid_records.append(record)
        unique_key = "".join([record['employee_id'], record['work_date'].replace("/", "_")])
        if unique_key not in punch_records:
            punch_records[unique_key] = []
        punch_records[unique_key].append({
            "punch_start_time": record["start_time"],
            "punch_end_time": record["end_time"],
            "work_date": record['work_date'],
            "project": record['project'],
            "task": record['task'],
            "start_time": record["start_time"],
            "end_time": record["end_time"],
            "employee_id": record['employee_id'],
        })
    return {
        "valid_records": valid_records,
        "invalid_records": invalid_records,
    }
    
def validate_mandatory_fields(csv_record: Dict[str, Any]) -> List[str]:
    missing_fields = []
    for field in MANDATORY_FIELDS:
        if not csv_record.get(field) or str(csv_record[field]).strip() == '':
            missing_fields.append(f"Blank {field.replace('_', ' ').title()} found in input file")

    work_date = csv_record.get('work_date')
    if work_date and str(work_date).strip():
        try:
            datetime.strptime(str(work_date).strip(), entry_dateformat)
        except Exception:
            missing_fields = [err for err in missing_fields if 'Blank Work Date' not in err]
            missing_fields.append('Invalid/Incorrect Work Date Format Received')
    if csv_record.get("activity"):
        missing_fields.append(f'Activity {csv_record.get("activity")} not present in replicon')
    return missing_fields

def validate_and_separate_records(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    valid_records = []
    invalid_records = []
    break_lunch_records = []
    
    for record in records:
        timesheet_category = str(record.get('timesheet_category', '')).strip().upper()
        if timesheet_category in ['BREAK', 'LUNCH']:
            record['activity'] = 'N/A'
            break_lunch_records.append(record)
        else:
            validation_errors = validate_mandatory_fields(record)
            if validation_errors:
                invalid_records.append(record)
            else:
                valid_records.append(record)
    
    return {
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'break_lunch_records': break_lunch_records
    }

def get_validation_error_message(record: Dict[str, Any]) -> str:
    original_record = {
        'Employee ID': record.get('employee_id', ''),
        'Work Date': record.get('work_date', '')
    }
    errors = validate_mandatory_fields(record)
    if errors:
        return "; ".join(errors)
    return "No validation errors"

def get_submitted_timesheet_uris(timesheet_data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    submitted_uris = set()
    
    for timesheet in timesheet_data:
        if timesheet.get('timesheet_status') != 'Not Submitted':
            timesheet_uri = timesheet.get('timesheet_uri')
            if timesheet_uri:
                submitted_uris.add(timesheet_uri)
    
    return list(map(lambda ts: {
        'ts_uri': ts
    }, list(submitted_uris)))


def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def get_status(item: Dict[str, Any], logstatus: str) -> bool:
    return item['status'].lower() == logstatus

def do_format_logs(dag_run) -> List[Dict[str, Any]]:
    log_artifacts = []
    log_records = []
 
    timeentrylogs = dag_run.conf.get('timeentrylogs')
    otherlogs = dag_run.conf.get('otherlogs')
 
    if timeentrylogs:
        if isinstance(timeentrylogs, list):
            log_artifacts.extend(timeentrylogs)
        elif isinstance(timeentrylogs, str) and timeentrylogs[0] == '[':
            timeentrylogs = literal_eval(timeentrylogs)
            log_artifacts.extend(timeentrylogs)
        else:
            log_artifacts.append(timeentrylogs)
 
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
 
    final_log_record = []
    for item in log_records:
        final_log_record.append({
            'employee_id': item['properties'].get('employee_id', ''),
            'work_date': item['properties'].get('work_date', ''),
            'project': item['properties'].get('project', ''),
            'task': item['properties'].get('task', ''),
            'activity': item['properties'].get('activity', ''),
            'action': item['properties'].get('action', ''),
            'status': item['properties'].get('status', ''),
            "details": item['properties'].get('details', ''),
            'ecid': item['ecid'],
        })
        
    final_log_records = []
    for record in final_log_record:
        final_log_records.append(record) 
        
    rail.set_result(key="error_record_count",
                    val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",
                    val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",
                    val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
 
    return final_log_records

def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.max
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_email_details(timezone, log_file_path, dag_run):
    current_time = now(timezone)
    start_time_str = dag_run.conf['start_time']
    filename = dag_run.conf['input_filename']
    log_filename = dag_run.conf['log_filename']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.isoformat(),
        "job_duration_minutes": (((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).seconds)//60),
        "log_timestamp": current_time.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_file_name": log_filename,
        "log_filepath": log_file_path,
        "input_filename": filename,
    }

def get_aggregate_seconds_for_activity():
    records = rail.load_all_records(rail.result('query_project_task_records'))
    base_table_records_for_workdate = sorted(
        rail.load_all_records(rail.result('get_all_user_punches_for_date')),
        key=lambda x: datetime.strptime(x["start_time"], "%I:%M:%S %p")
    )
    activity_seconds = {}
 
    base_map = {item['start_time']: (i, item) for i, item in enumerate(base_table_records_for_workdate)}
 
    for record in records:
        activity = record["activity"]
        category = (record.get("timesheet_category") or "").lower()
 
        if activity == "N/A" and category in ["break", "lunch"]:
            activity = category.capitalize()
 
        start_time = datetime.strptime(record["start_time"], "%I:%M:%S %p")
        end_time = datetime.strptime(record["end_time"], "%I:%M:%S %p")
        duration = (end_time - start_time).seconds
 
        matching_record_tuple = base_map.get(record["start_time"])
        if not matching_record_tuple:
            continue
        index_matching_record, matching_record = matching_record_tuple
 
        is_transfer = False
        next_record_start_time = base_table_records_for_workdate[index_matching_record + 1].get("start_time") if index_matching_record + 1 < len(base_table_records_for_workdate) else None
 
        if index_matching_record > 0:
            previous_record = base_table_records_for_workdate[index_matching_record - 1]
            if (previous_record["timesheet_category"].lower() not in ["break", "lunch"]) and (
                previous_record["end_time"] == record["start_time"]):
                is_transfer = True
        key = record["project"] + record["task"] + activity
        if key not in activity_seconds:
            activity_seconds[key] = {
                "employee_id": record["employee_id"],
                "work_date": record["work_date"],
                "project": record["project"],
                "task": record["task"],
                "project_uri": record.get("project_uri"),
                "task_uri": record.get("task_uri"),
                "activity": activity,
                "total_seconds": 0,
                "punch_records": []
            }
 
        activity_seconds[key]["punch_records"].append({
            "punch_start_time": record["start_time"],
            "punch_end_time": record["end_time"],
            "timesheet_category": record["timesheet_category"],
            "is_transfer": is_transfer,
            "next_record_start_time": next_record_start_time
        })

        if category not in ["break", "lunch"]:
            activity_seconds[key]["total_seconds"] += duration
 
    return list(activity_seconds.values())

def is_any_task_closed(dag_run):
            tasks = rail.result('get_all_tasks_for_project')
            input_task = dag_run.conf['task']

            if not tasks or not input_task:
                return False    
            levels = [lvl.strip() for lvl in input_task.split('/')]

            current_path = ""

            for level in levels:
                current_path = f"{current_path}|{level}" if current_path else level

                matched_task = next(
                    (t for t in tasks if t.get("full_task_name") == current_path),
                    None
                )

                if not matched_task:
                    return False

                if matched_task.get("isclosed", False):
                    return True

            return False