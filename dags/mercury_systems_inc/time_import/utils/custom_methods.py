from datetime import datetime
from functools import lru_cache
import rail

# Constants
DATE_FORMAT = "%m%d%Y"
SQL_DATEFORMAT = "%Y-%m-%d"

def validate_csv_record(item):
    """Validate a CSV record for required fields"""
    missing_fields = []
    required_fields = {
        'EmployeeID': 'employee_id',
        'EntryDate': 'entry_date',
        'Hours': 'hours',
        'ProjectCode': 'project_code',
        'TaskCode': 'task_code'
    }

    for field_name, field_key in required_fields.items():
        if not item[field_key]:
            missing_fields.append(f"{field_name} is missing")
            continue
        if field_key == "hours" and float(item[field_key]) <= 0:
            missing_fields.append(f"{field_name} are not valid")
        if field_key == "entry_date" and item["entry_date"] and rail.result("get_run_date") != item["entry_date"]:
            entry_date = item["entry_date"]
            try:
                if datetime.strptime(entry_date, SQL_DATEFORMAT):
                    missing_fields.append(f"Entry date not the same as current date")
            except ValueError:
                missing_fields.append("Entry date is not valid the expected format is MMDDYYYY")
                    
    return missing_fields

def format_logs():
    """Format logs for output"""
    formatted_logs = []
    logs = rail.result('get_time_entry_import_logs') or []
    if logs:
        for timeentry in logs:
            log_records = rail.load_all_records(timeentry)
            for log in log_records:
                if isinstance(log, dict) and 'properties' in log:
                    formatted_logs.append(
                        {**log['properties'], "ecid": log.get("ecid", "")})
    main_logs = rail.load_all_records(rail.result(
        "create_main_log")) if rail.result("create_main_log") else []
    for log in main_logs:
        if isinstance(log, dict) and 'properties' in log:
            formatted_logs.append(
                {**log['properties'], "ecid": log.get("ecid", "")})

    # Count different log types
    success_count = len(
        list(filter(lambda log: log.get('status') == 'Success', formatted_logs)))
    error_count = len(
        list(filter(lambda log: log.get('status') == 'Error', formatted_logs)))
    exception_count = len(
        list(filter(lambda log: log.get('status') == 'Exception', formatted_logs)))

    return {
        'logs': formatted_logs,
        'success_count': success_count,
        'error_count': error_count,
        'exception_count': exception_count,
        'total_count': len(formatted_logs)
    }

def get_invalid_user_message(item):
    if item["employee_id_in_repl"] == "0":
        return "Employee does not exist in Replicon"
    if item["user_status"] != "Enabled":
        return "User is disabled in Replicon"
    if not item["timesheet_template"]:
        return "Timesheet not assigned to the user"
    if "No Distribution" in item["timesheet_template"]:
        return "Invalid timesheet with no distribution is assigned to the user"
    return "Multiple users with same Employee ID"


@lru_cache(maxsize=128)
def get_date(entry_date):
    try:
        if entry_date and datetime.strptime(entry_date, DATE_FORMAT):
            return datetime.strftime(datetime.strptime(entry_date, DATE_FORMAT),SQL_DATEFORMAT)
    except ValueError:
        return entry_date

def get_sqldate_for_import_records(item):
    if not item:
        return []
    return {
        **item,
        "entry_date": get_date(item["entry_date"])
    }