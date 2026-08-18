"""
Unisys Project Import - Custom Method Utilities
Implements Unisys-specific business logic and helper methods
"""
from datetime import datetime
import re
from os import path
import rail

def validate_project_file_name(file_path, expected_prefix):
    """
    Validate project import file name matches expected pattern
    Also checks if file is a resource assignment file (to avoid processing wrong files)

    Pattern: {PREFIX}_Project_task_YYYYMMDD_HHMMSS.csv.pgp
    Examples:
        - DEV_Project_task_20250127_143022.csv.pgp
        - UAT_Project_task_20250127_143022.csv.pgp
        - PROD_Project_task_20250127_143022.csv.pgp

    Args:
        file_path: Full SFTP file path
        expected_prefix: Environment prefix (DEV, UAT, PROD)

    Returns:
        dict: {
            'is_valid': bool,
            'is_resource_file': bool,
            'is_unknown_file': bool,
            'error_message': str
        }
    """
    file_name = path.basename(file_path)

    # Pattern: {PREFIX}_Project_task_YYYYMMDD_HHMMSS.csv.pgp
    project_pattern = rf'^{expected_prefix}_Project_task_\d{{8}}_\d{{6}}\.csv\.pgp$'

    # Pattern: {PREFIX}_Assignment_YYYYMMDD_HHMMSS.csv.pgp
    resource_pattern = rf'^{expected_prefix}_Assignment_\d{{8}}_\d{{6}}\.csv\.pgp$'

    if re.match(project_pattern, file_name):
        return {
            'is_valid': True,
            'is_resource_file': False,
            'is_unknown_file': False,
            'error_message': ''
        }
    elif re.match(resource_pattern, file_name):
        # This is a resource assignment file, not a project file
        return {
            'is_valid': False,
            'is_resource_file': True,
            'is_unknown_file': False,
            'error_message': f"File is a resource assignment file: {file_name}. This will be processed by resource assignment DAG."
        }
    else:
        # File doesn't match either pattern - truly invalid
        return {
            'is_valid': False,
            'is_resource_file': False,
            'is_unknown_file': True,
            'error_message': f"Invalid file name: {file_name}. Expected format: {expected_prefix}_Project_task_YYYYMMDD_HHMMSS.csv.pgp or {expected_prefix}_Assignment_YYYYMMDD_HHMMSS.csv.pgp"
        }

mandatory_fields = {
    "project_fields": {
        "projectnumber": "projectnumber",
        "projectname": "projectname",
        "projectstartdate": "projectstartdate",
        "projectstatus": "projectstatus",
        "taskcode": "taskcode",
        "taskname": "taskname",
        "taskstartdate": "taskstartdate",
        "taskenddate": "taskenddate",
        'companycode': 'companycode'
    }
}


def validate_project_dates_only(project_data, DATE_FORMAT_INPUT):
    errors = []
    project_start = None
    project_end = None
    project_number = project_data.get('projectnumber', 'Unknown')

    # Validate project start date format
    if project_data.get('projectstartdate'):
        try:
            project_start = datetime.strptime(project_data['projectstartdate'].upper(), DATE_FORMAT_INPUT)
        except ValueError as e:
            date_str = project_data['projectstartdate']
            # Check if it's an invalid day (e.g., 32-JAN-2024)
            if any(char.isdigit() for char in date_str):
                try:
                    day = int(date_str.split('-')[0])
                    if day > 31:
                        errors.append(f"Project {project_number} is skipped due to invalid start date format received '{date_str}', day value cannot exceed 31")
                    else:
                        errors.append(f"Project {project_number} is skipped due to invalid start date format received '{date_str}', expected format like '15-JAN-2024'")
                except:
                    errors.append(f"Project {project_number} is skipped due to invalid start date format received '{date_str}', expected format like '15-JAN-2024'")
            else:
                errors.append(f"Project {project_number} is skipped due to invalid start date format received '{date_str}', use valid month abbreviation like JAN, FEB, MAR")

    # Validate project end date format
    if project_data.get('projectenddate'):
        try:
            project_end = datetime.strptime(project_data['projectenddate'].upper(), DATE_FORMAT_INPUT)
        except ValueError as e:
            date_str = project_data['projectenddate']
            # Check if it's an invalid day (e.g., 32-DEC-2024)
            if any(char.isdigit() for char in date_str):
                try:
                    parts = date_str.split('-')
                    day = int(parts[0])
                    month_str = parts[1].upper() if len(parts) > 1 else ''

                    # Month-specific validation
                    month_days = {
                        'JAN': 31, 'FEB': 29, 'MAR': 31, 'APR': 30, 'MAY': 31, 'JUN': 30,
                        'JUL': 31, 'AUG': 31, 'SEP': 30, 'OCT': 31, 'NOV': 30, 'DEC': 31
                    }

                    if month_str in month_days and day > month_days[month_str]:
                        month_name = {'JAN': 'January', 'FEB': 'February', 'MAR': 'March', 'APR': 'April',
                                     'MAY': 'May', 'JUN': 'June', 'JUL': 'July', 'AUG': 'August',
                                     'SEP': 'September', 'OCT': 'October', 'NOV': 'November', 'DEC': 'December'}
                        errors.append(f"Project {project_number} is skipped due to invalid end date format received '{date_str}', {month_name[month_str]} has maximum {month_days[month_str]} days")
                    else:
                        errors.append(f"Project {project_number} is skipped due to invalid end date format received '{date_str}', expected format like '15-JAN-2024'")
                except:
                    errors.append(f"Project {project_number} is skipped due to invalid end date format received '{date_str}', expected format like '15-JAN-2024'")
            else:
                errors.append(f"Project {project_number} is skipped due to invalid end date format received '{date_str}', use valid month abbreviation like JAN, FEB, MAR")

    # Validate project date range logic (only if both dates are valid)
    if project_start and project_end:
        if project_end < project_start:
            errors.append(f"Project {project_number} is skipped due to invalid date sequence, end date '{project_data['projectenddate']}' received before start date '{project_data['projectstartdate']}'")

    return {
        'is_valid': len(errors) == 0,
        'errors': errors,
        'project_start': project_data.get('projectstartdate'),
        'project_end': project_data.get('projectenddate')
    }


def validate_task_dates(task_data, DATE_FORMAT_INPUT, project_start=None, project_end=None):
    """
    Validate task dates with format validation and before/after logic

    Validations:
    1. Task start and end dates are in valid format (DD-MMM-YYYY)
    2. Task end date >= task start date
    3. Task dates fall within project dates (if project dates provided)

    Args:
        task_data: Task dictionary with taskstartdate and taskenddate
        DATE_FORMAT_INPUT: Date format string (e.g., "%d-%b-%Y")
        project_start: Project start date STRING (DD-MMM-YYYY format)
        project_end: Project end date STRING (DD-MMM-YYYY format)

    Returns:
        dict: {
            'is_valid': bool,
            'task': original task data,
            'error': error message if invalid, None if valid
        }
    """

    result = {
        'is_valid': True,
        'task': task_data,
        'error': None
    }

    task_code = task_data.get('taskcode', 'Unknown')
    project_number = task_data.get('projectnumber', 'Unknown')
    task_start = None
    task_end = None

    # Validate task start date format
    if task_data.get('taskstartdate'):
        try:
            task_start = datetime.strptime(task_data['taskstartdate'].upper(), DATE_FORMAT_INPUT)
        except ValueError:
            date_str = task_data['taskstartdate']
            # Check if it's an invalid day (e.g., 32-JAN-2024)
            if any(char.isdigit() for char in date_str):
                try:
                    parts = date_str.split('-')
                    day = int(parts[0])
                    month_str = parts[1].upper() if len(parts) > 1 else ''

                    month_days = {
                        'JAN': 31, 'FEB': 29, 'MAR': 31, 'APR': 30, 'MAY': 31, 'JUN': 30,
                        'JUL': 31, 'AUG': 31, 'SEP': 30, 'OCT': 31, 'NOV': 30, 'DEC': 31
                    }

                    if month_str in month_days and day > month_days[month_str]:
                        month_name = {'JAN': 'January', 'FEB': 'February', 'MAR': 'March', 'APR': 'April',
                                     'MAY': 'May', 'JUN': 'June', 'JUL': 'July', 'AUG': 'August',
                                     'SEP': 'September', 'OCT': 'October', 'NOV': 'November', 'DEC': 'December'}
                        result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid start date format received '{date_str}', {month_name[month_str]} has maximum {month_days[month_str]} days"
                    else:
                        result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid start date format received '{date_str}', expected format like '15-JAN-2024'"
                except:
                    result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid start date format received '{date_str}', expected format like '15-JAN-2024'"
            else:
                result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid start date format received '{date_str}', use valid month abbreviation like JAN, FEB, MAR"

            result['is_valid'] = False
            return result

    # Validate task end date format
    if task_data.get('taskenddate'):
        try:
            task_end = datetime.strptime(task_data['taskenddate'].upper(), DATE_FORMAT_INPUT)
        except ValueError:
            date_str = task_data['taskenddate']
            # Check if it's an invalid day (e.g., 32-JAN-2024)
            if any(char.isdigit() for char in date_str):
                try:
                    parts = date_str.split('-')
                    day = int(parts[0])
                    month_str = parts[1].upper() if len(parts) > 1 else ''

                    month_days = {
                        'JAN': 31, 'FEB': 29, 'MAR': 31, 'APR': 30, 'MAY': 31, 'JUN': 30,
                        'JUL': 31, 'AUG': 31, 'SEP': 30, 'OCT': 31, 'NOV': 30, 'DEC': 31
                    }

                    if month_str in month_days and day > month_days[month_str]:
                        month_name = {'JAN': 'January', 'FEB': 'February', 'MAR': 'March', 'APR': 'April',
                                     'MAY': 'May', 'JUN': 'June', 'JUL': 'July', 'AUG': 'August',
                                     'SEP': 'September', 'OCT': 'October', 'NOV': 'November', 'DEC': 'December'}
                        result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid end date format received '{date_str}', {month_name[month_str]} has maximum {month_days[month_str]} days"
                    else:
                        result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid end date format received '{date_str}', expected format like '15-JAN-2024'"
                except:
                    result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid end date format received '{date_str}', expected format like '15-JAN-2024'"
            else:
                result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid end date format received '{date_str}', use valid month abbreviation like JAN, FEB, MAR"

            result['is_valid'] = False
            return result

    # Validate task end >= task start
    if task_start and task_end:
        if task_end < task_start:
            result['is_valid'] = False
            result['error'] = f"Task {task_code} in Project {project_number} is skipped due to invalid date sequence, end date '{task_data['taskenddate']}' received before start date '{task_data['taskstartdate']}'"
            return result

    # Parse project dates from strings to datetime for comparison
    project_start_dt = None
    project_end_dt = None

    if project_start:
        try:
            project_start_dt = datetime.strptime(project_start.upper(), DATE_FORMAT_INPUT)
        except (ValueError, AttributeError):
            # If project_start is not a valid string, skip validation
            project_start_dt = None

    if project_end:
        try:
            project_end_dt = datetime.strptime(project_end.upper(), DATE_FORMAT_INPUT)
        except (ValueError, AttributeError):
            # If project_end is not a valid string, skip validation
            project_end_dt = None

    # Validate task dates fall within project dates
    if project_start_dt and task_start:
        if task_start < project_start_dt:
            result['is_valid'] = False
            result['error'] = f"Task {task_code} in Project {project_number} is skipped due to task start date '{task_data['taskstartdate']}' falling before project start date '{project_start}'"
            return result

    if project_end_dt and task_end:
        if task_end > project_end_dt:
            result['is_valid'] = False
            result['error'] = f"Task {task_code} in Project {project_number} is skipped due to task end date '{task_data['taskenddate']}' falling after project end date '{project_end}'"
            return result

    return result

def get_invalid_logs_property_conf(item):
    """Generate log properties for validation errors"""
    def get_missing_field():
        missing_fields = []
        field_display_names = {
            'projectnumber': 'Project Number',
            'projectname': 'Project Name',
            'projectstartdate': 'Project Start Date',
            'projectstatus': 'Project Status',
            'taskcode': 'Task Code',
            'taskname': 'Task Name',
            'taskstartdate': 'Task Start Date',
            'taskenddate': 'Task End Date',
            'companycode': 'Company Code'
        }

        for field_key, field_name in mandatory_fields['project_fields'].items():
            if item.get(field_key) in [None, '']:
                missing_fields.append(field_display_names.get(field_key, field_key))

        # Check project status separately (case-insensitive)
        project_status = item.get('projectstatus', '').strip().upper()
        if project_status not in ['ACTIVE', 'CLOSED']:
            status = item.get('projectstatus', 'blank')
            return f"Project {item.get('projectnumber', 'Unknown')} is skipped due to invalid project status '{status}' received, only 'Active' or 'Closed' allowed"

        if missing_fields:
            fields_str = ' and '.join(missing_fields) if len(missing_fields) <= 2 else ', '.join(missing_fields[:-1]) + ' and ' + missing_fields[-1]
            return f"Project processing is skipped due to required fields missing in CSV file: {fields_str}"

        return "Project processing is skipped due to required fields missing in CSV file"

    return {
        "projectnumber": item.get('projectnumber', ''),
        "projectname": item.get('projectname', ''),
        "taskcode": item.get('taskcode', ''),
        "taskname": item.get('taskname', ''),
        'action': 'Validation',
        "details": get_missing_field(),
        "status": 'Exception'
    }


def format_logs_callable():
    """
    Format and aggregate logs from all DAG runs
    Calculates success, error, and exception counts for email reporting
    """
    final_log_records = []
    final_log_records.extend(rail.load_all_records(rail.result("create_exception_log")))

    # Set counters for email template
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'exception', final_log_records))))

    return rail.write_json_artifact(final_log_records)


def get_task_to_add_update_skip(DATE_FORMAT_INPUT):
    """
    Categorize tasks for add/update/skip operations following CRL pattern
    Implements Unisys-specific validation, date validation, and field reversal logic
    """
    current_tasks_in_project = rail.result('get_all_tasks_for_project') if bool(rail.result('get_all_tasks_for_project')) else []
    tasks_to_process = rail.load_all_records(rail.result("get_project_data_from_query"))

    if not tasks_to_process:
        return {
            'add': [],
            'update': [],
            'skip': []
        }

    # Get project date range from validation result
    project_start = rail.result("validate_dates", {}).get('project_start')
    project_end = rail.result("validate_dates", {}).get('project_end')

    if not current_tasks_in_project:
        # No existing tasks, all are new - validate and categorize
        return {
            'add': validate_tasks_for_addition(tasks_to_process, DATE_FORMAT_INPUT, project_start, project_end),
            'update': [],
            'skip': []
        }

    tasks_to_add = []
    tasks_to_update = []
    tasks_to_skip = []

    for task in tasks_to_process:
        task_code = task.get('taskcode', 'Unknown')
        project_number = task.get('projectnumber', 'Unknown')
        task_name_len = len(task.get('taskname', ''))

        # Unisys validation: Max 50 chars for task name (skip if longer)
        if task_name_len > 50:
            tasks_to_skip.append({
                "task": task,
                "message": f"Task {task_code} in Project {project_number} is skipped due to task name length exceeding limit, received {task_name_len} characters but maximum 50 allowed",
                "taskcode": task_code,
                "taskname": task.get('taskname', ''),
                "action": "Skip",
                "status": "Skipped"
            })
            continue

        # Validate task dates
        task_validation = validate_task_dates(task, DATE_FORMAT_INPUT, project_start, project_end)
        if not task_validation['is_valid']:
            tasks_to_skip.append({
                "task": task,
                "message": task_validation['error'],
                "taskcode": task_code,
                "taskname": task.get('taskname', ''),
                "action": "Skip",
                "status": "Skipped"
            })
            continue

        # Find existing task using field reversal logic
        # In Replicon: 'name' field stores CSV taskcode (due to field reversal)
        existing_task = rail.find_first_by_attr_and_get_attr(
            current_tasks_in_project, "name", task['taskcode'])

        if existing_task:
            # Task exists, check if update needed
            if can_update_task_unisys(existing_task, task):
                task['existing_uri'] = existing_task['uri']
                tasks_to_update.append(task)
            else:
                tasks_to_skip.append({
                    "task": task,
                    "message": f"Task {task_code} in Project {project_number} is skipped due to no data changes detected, existing Replicon task already matches CSV values",
                    "taskcode": task_code,
                    "taskname": task.get('taskname', ''),
                    "action": "Skip",
                    "status": "Skipped"
                })
        else:
            # New task
            tasks_to_add.append(task)

    return {
        'add': rail.load_all_records(rail.write_json_artifact(tasks_to_add)) if tasks_to_add else tasks_to_add,
        'update': rail.load_all_records(rail.write_json_artifact(tasks_to_update)) if tasks_to_update else tasks_to_update,
        'skip': rail.load_all_records(rail.write_json_artifact(tasks_to_skip)) if tasks_to_skip else tasks_to_skip
    }


def can_update_task_unisys(existing_task, csv_task):
    """
    Determine if task needs update based on Unisys business rules

    Field reversal reminder:
    - Replicon task 'name' field contains CSV taskcode
    - Replicon task 'code' field contains CSV taskname
    """
    # Check if task name needs update (CSV taskname -> Replicon code)
    if existing_task.get('code') != csv_task.get('taskname'):
        return True

    # Check date changes
    existing_start = format_date_for_comparison(existing_task.get('startDate'))
    existing_end = format_date_for_comparison(existing_task.get('endDate'))
    csv_start = format_date_for_comparison(csv_task.get('taskstartdate'))
    csv_end = format_date_for_comparison(csv_task.get('taskenddate'))

    if existing_start != csv_start or existing_end != csv_end:
        return True

    # Check paycode changes
    existing_paycode = existing_task.get('paycode', '105')
    csv_paycode = csv_task.get('taskpaycode', '105')
    if existing_paycode != csv_paycode:
        return True

    return False


def validate_tasks_for_addition(tasks, DATE_FORMAT_INPUT, project_start=None, project_end=None):
    """
    Validate new tasks before addition
    Includes task name length and date validation
    """
    valid_tasks = []
    for task in tasks:
        # Skip tasks with names longer than 50 characters
        if len(task.get('taskname', '')) > 50:
            continue

        # Validate task dates
        task_validation = validate_task_dates(task, DATE_FORMAT_INPUT, project_start, project_end)
        if task_validation['is_valid']:
            valid_tasks.append(task)

    return valid_tasks


def format_date_for_comparison(date_string):
    """Normalize dates for comparison"""
    if not date_string:
        return None

    try:
        # Try different date formats
        for fmt in ['%Y-%m-%d', '%d-%b-%Y', '%d-%B-%Y']:
            try:
                dt = datetime.strptime(date_string.upper(), fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return date_string
    except:
        return date_string


def map_task_success_error(task_id, _type):
    action = "Added" if _type == "add" else "Updated"
    batched_results = rail.result(task_id, [])
    task_list = rail.result("get_all_task_to_add_update", {}).get(_type, [])

    flattened_results = []
    if isinstance(batched_results, list) and batched_results:
        # Check if this is batched results (list of lists)
        if isinstance(batched_results[0], list):
            # Flatten the nested lists
            for batch_result in batched_results:
                flattened_results.extend(batch_result)
        else:
            # Single batch or legacy format
            flattened_results = batched_results
    else:
        flattened_results = []

    res = []
    for idx, task_res in enumerate(flattened_results):
        if idx >= len(task_list):
            break

        task_detail = task_list[idx].copy()
        status = "Success"
        msg = f"Task {action} Successfully"

        if task_res.get("error"):
            msg = ";".join([error.get('displayText', '')
                           for error in task_res.get("error", {}).get('notifications', [])])
            status = "Error"

        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_detail)

    return res

def split_list_into_batches(items, batch_size=500):
    batches = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batches.append(batch)
    return batches


def get_batched_tasks(action_type, batch_size=500):
    task_data = rail.result("get_all_task_to_add_update")
    tasks = task_data.get(action_type, [])

    if not tasks:
        return []

    return split_list_into_batches(tasks, batch_size)
