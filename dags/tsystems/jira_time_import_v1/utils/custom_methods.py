from datetime import datetime, timedelta
import json
from dateutil.relativedelta import relativedelta
from pendulum import now
import rail

null = None

SQL_DATEFORMAT = "%Y-%m-%d"
REP_DATE_FORMAT = "%m/%d/%Y"

def get_date(entry_date, date_format):
    """Convert T-Systems date format to SQL format YYYY-MM-DD"""
    try:
        if entry_date and datetime.strptime(entry_date, date_format):
            return datetime.strftime(datetime.strptime(entry_date, date_format), SQL_DATEFORMAT)
    except ValueError:
        return null
    return null

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
    """Validate a CSV record for required fields"""
    missing_fields = []
    required_fields = {
        'ID': 'unique_id',
        'Login': 'employee_id',
        'Work_Date': 'entry_date',
        'Work_Hours': 'hours',
        'WBS_Code': 'project_id',
        'Task_Name': 'full_task_path'
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

def check_user_assigned_to_project(dag_run):
    """
    Checks if user is assigned to the project team.
    
    Args:
        project_details (dict): Project details payload containing team info
        
    Returns:
        bool: True if user is assigned to project, False otherwise
    """
    project_details = rail.result('get_all_project_details')
    user_uri = dag_run.conf.get('user_uri', '')
    
    # Check if project_details has team information
    if not project_details or 'team' not in project_details:
        return False
        
    # Find the user in the team
    for team_member in project_details.get('team', []):
        # Check if this team member's user URI matches
        resource = team_member.get('resource')
        if resource:
            # Check if user URI matches the resource's user URI
            user = resource.get('user')
            if user and user.get('uri') == user_uri:
                return True
            # Check if user URI matches the resource URI directly
            if resource.get('uri') == user_uri:
                return True
    
    return False

def check_billing_rate_allowed_for_user(dag_run):
    """
    Checks if any billing rate is assigned to the user on this project.
    
    Args:
        project_details (dict): Project details payload containing team and billing rate info
        
    Returns:
        bool: True if any billing rate is assigned to user, False otherwise
    """
    project_details = rail.result('get_all_project_details')
    user_uri = dag_run.conf.get('user_uri', '')

    if project_details["project"]["billingType"]["uri"] == "urn:replicon:billing-type:non-billable":
        return True
    
    # Check if project_details has team information
    if not project_details or 'team' not in project_details:
        return False
        
    # Find the user in the team
    for team_member in project_details.get('team', []):
        # Check if this team member's user URI matches
        resource = team_member.get('resource')
        if resource:
            # Check if user URI matches the resource's user URI
            user = resource.get('user')
            user_found = False
            
            if user and user.get('uri') == user_uri:
                user_found = True
            elif resource.get('uri') == user_uri:
                user_found = True
            
            if user_found:
                # User found in team, now check if any billing rates are assigned
                billing_rates = team_member.get('billingRatesAllowedForBillingTime', [])
                
                # Return True if user has any billing rates assigned
                return len(billing_rates) > 0
    
    # User not found in team (shouldn't reach here if check_user_assigned_to_project was called first)
    return False

def get_email_log_details(log_file_path, dag_run, STANDARD_EMAIL_DATE_FORMAT):
    """
    Generate detailed email notification metadata for time import process.
    
    Creates a dictionary containing timestamps, duration, and file paths
    for use in email notifications. Calculates process duration and formats
    timestamps according to standard formats.
    
    Args:
        timezone (str): Timezone identifier for timestamp generation
        log_file_path (str): Path to the generated log file
        dag_run: Airflow DAG run context containing process metadata
        
    Returns:
        dict: Email notification details including:
            - start_time: ISO format timestamp when process started
            - job_end_time: ISO format timestamp when process completed
            - job_duration_minutes: Process duration in minutes
            - log_timestamp: Compact timestamp for log reference
            - email_timestamp: Full ISO timestamp for email headers
            - log_file_name: Name of the generated log file
            - log_filepath: Full path to the log file
            - input_filename: Original input file name
    """
    current_time = now()
    start_time_str = dag_run.conf['start_time']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_filepath": log_file_path,
        "input_filename": dag_run.conf['source_filename'],
        "total_record_count": dag_run.conf['total_record_count']
    }

def get_submitted_timesheet_uris():
    """
    Extract unique timesheet URIs for timesheets that need reopening.
    
    Identifies timesheets that are in submitted, approved, or other non-open states
    that need to be reopened before time entries can be modified.
    
    Args:
        timesheet_data: List of dictionaries containing timesheet status information
        
    Returns:
        List[Dict[str, str]]: List of dictionaries with 'ts_uri' key containing
                              unique timesheet URIs that need reopening
    """
    timesheet_data = rail.result('get_timesheet_details')
    submitted_uris = set()
    
    for timesheet in timesheet_data:
        if timesheet.get('timesheet_status') != 'Not Submitted':
            timesheet_uri = timesheet.get('timesheet_uri')
            if timesheet_uri:
                submitted_uris.add(timesheet_uri)
    
    return list(map(lambda ts: {
        'ts_uri': ts
    }, list(submitted_uris)))

def do_format_logs(dag_run):
    """Format logs for output grouped by users and project managers"""
    all_logs = []
    logs = dag_run.conf['timeentrylogs'] + [dag_run.conf['otherlogs']]

    if logs:
        for timeentry in logs:
            log_records = rail.load_all_records(timeentry)
            for log in log_records:
                if isinstance(log, dict) and 'properties' in log:
                    all_logs.append(
                        {**log['properties'], "ecid": log.get("ecid", "")})
    main_logs = rail.load_all_records(rail.result(
        "create_main_log")) if rail.result("create_main_log") else []
    for log in main_logs:
        if isinstance(log, dict) and 'properties' in log:
            all_logs.append(
                {**log['properties'], "ecid": log.get("ecid", "")})
    
    # Set result counts
    rail.set_result(key="error_record_count", val=len(list(filter(lambda item: item['status'] == "Error", all_logs))))
    rail.set_result(key="success_record_count", val=len(list(filter(lambda item: item['status'] == "Success", all_logs))))
    rail.set_result(key="exception_record_count", val=len(list(filter(lambda item: item['status'] == "Exception", all_logs))))
    
    # Group logs by users and project managers
    users = {}
    project_managers = {}
    
    for log in all_logs:
        # Group by user email
        user_email = log.get('employee_email', '')
        # Skip invalid emails: empty, None, null, or "None" string
        if (user_email and 
            user_email.strip() and 
            user_email.lower() not in ['none', 'null'] and
            '@' in user_email):  # Basic email validation
            if user_email not in users:
                users[user_email] = {
                    'logs': [],
                    'success_count': 0,
                    'error_count': 0,
                    'exception_count': 0,
                    'total_records': 0
                }
            users[user_email]['logs'].append(log)
            # Update counts
            users[user_email]['total_records'] += 1
            if log.get('status') == 'Success':
                users[user_email]['success_count'] += 1
            elif log.get('status') == 'Error':
                users[user_email]['error_count'] += 1
            elif log.get('status') == 'Exception':
                users[user_email]['exception_count'] += 1
        
        # Group by project manager email (only for errors/exceptions)
        if log.get('status') in ['Error', 'Exception']:
            pm_email = log.get('project_manager_email', '')
            # Skip invalid emails: empty, None, null, or "None" string
            if (pm_email and 
                pm_email.strip() and 
                pm_email.lower() not in ['none', 'null'] and
                '@' in pm_email):  # Basic email validation
                if pm_email not in project_managers:
                    project_managers[pm_email] = {
                        'logs': [],
                        'error_count': 0,
                        'exception_count': 0,
                        'total_records': 0
                    }
                project_managers[pm_email]['logs'].append(log)
                # Update counts
                project_managers[pm_email]['total_records'] += 1
                if log.get('status') == 'Error':
                    project_managers[pm_email]['error_count'] += 1
                elif log.get('status') == 'Exception':
                    project_managers[pm_email]['exception_count'] += 1
    
    formatted_logs = {
        'users': users,
        'project_managers': project_managers,
        'all_logs': {
            'logs': all_logs,
            'success_count': len([log for log in all_logs if log.get('status') == 'Success']),
            'error_count': len([log for log in all_logs if log.get('status') == 'Error']),
            'exception_count': len([log for log in all_logs if log.get('status') == 'Exception']),
            'total_records': len(all_logs)
        }
    }
    
    return rail.write_json_artifact(formatted_logs)

def join_user_and_project_details():
    """
    Join processed records with project details and user details.
    Handles potential duplicates in the lookup tables by using the first match.
    """
    # Load the collections
    processed_records = rail.load_all_records(rail.result('create_processed_input_data_collection'))
    project_details = rail.load_all_records(rail.result('create_project_details_collection'))
    user_details = rail.load_all_records(rail.result('create_user_details_collection'))
    
    # Create lookup dictionaries to ensure one entry per key
    project_lookup = {}
    for project in project_details:
        project_code = project.get('Project_Code')
        if project_code and project_code not in project_lookup:
            project_lookup[project_code] = {
                'project_manager_name': project.get('Project_Manager'),
                'project_manager_email': project.get('Project_Manager_Email')
            }
    
    user_lookup = {}
    for user in user_details:
        employee_id = user.get('Employee_ID')
        if employee_id and employee_id not in user_lookup:
            user_lookup[employee_id] = {
                'username': user.get('User_Name'),
                'user_email': user.get('User_Email')
            }
    
    # Join the data
    enriched_records = []
    for record in processed_records:
        enriched_record = dict(record)  # Create a copy
        
        # Add project details
        project_id = record.get('project_id')
        if project_id and project_id in project_lookup:
            enriched_record['project_manager_name'] = project_lookup[project_id]['project_manager_name']
            enriched_record['project_manager_email'] = project_lookup[project_id]['project_manager_email']
        else:
            enriched_record['project_manager_name'] = ''
            enriched_record['project_manager_email'] = ''
        
        # Add user details
        employee_id = record.get('employee_id')
        if employee_id and employee_id in user_lookup:
            enriched_record['username'] = user_lookup[employee_id]['username']
            enriched_record['user_email'] = user_lookup[employee_id]['user_email']
        else:
            enriched_record['username'] = ''
            enriched_record['user_email'] = ''
        
        enriched_records.append(enriched_record)
    
    return json.dumps(enriched_records)

def validate_user_records_date_and_hours():
    """
    Validate user records for date range (M-1 to 30 days) and 24-hour limit per user per date.
    Returns dict with valid_records and invalid_records.
    """
    all_records = rail.load_all_records(rail.result('get_all_records_for_user'))
    
    # Calculate date boundaries
    current_date = now()
    # First day of previous month
    first_day_previous_month = current_date.date() + relativedelta(months=-1, day=1)
    # 30 days from today
    thirty_days_from_now = current_date.date() + timedelta(days=30)
    
    # Dictionary to track hours per user per date
    user_date_hours = {}
    valid_records = []
    invalid_records = []
    
    # Process all records
    for record in all_records:
        validation_error = None
        
        # Check date range validation
        entry_date_sql = record.get('entry_date_sql')
        if entry_date_sql:
            try:
                entry_date = datetime.strptime(entry_date_sql, SQL_DATEFORMAT).date()
                
                # Check if date is before first day of previous month
                if entry_date < first_day_previous_month:
                    validation_error = 'Time entries prior to M-1 are not allowed'
                
                # Check if date is after 30 days from today
                elif entry_date > thirty_days_from_now:
                    validation_error = 'Timesheet does not exist'
                    
            except ValueError:
                # Skip records with invalid dates (already handled in basic validation)
                pass
        
        # If no date validation error, check 24-hour limit
        if not validation_error:
            employee_id = record.get('employee_id', '')
            entry_date = record.get('entry_date', '')
            hours_str = record.get('hours', '0')
            
            try:
                hours = float(hours_str)
            except (ValueError, TypeError):
                hours = 0
                
            key = f"{employee_id}_{entry_date}"
            
            if key not in user_date_hours:
                user_date_hours[key] = 0
                
            # Check if adding this record would exceed 24 hours
            if user_date_hours[key] + hours > 24:
                validation_error = 'If more than 24 hours are identified in the file, the hours will not be updated.'
            else:
                # Only accumulate hours if within limit
                user_date_hours[key] += hours
        
        # Add to appropriate list
        if validation_error:
            record['validation_error'] = validation_error
            invalid_records.append(record)
        else:
            valid_records.append(record)

    return {
        'valid_count': len(valid_records),
        'invalid_count': len(invalid_records),
        'valid_records': rail.write_json_artifact(valid_records),
        'invalid_records': rail.write_json_artifact(invalid_records)
    }
