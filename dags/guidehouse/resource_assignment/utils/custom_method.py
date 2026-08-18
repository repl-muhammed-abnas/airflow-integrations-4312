from datetime import datetime
import re
from os import path
import rail

def validate_assignment_file_name(file_path, expected_prefix, project_prefix):
    """
    Classify a file picked up from the shared inbound SFTP folder.

    Patterns:
        - Resource assignment: <expected_prefix>_YYYYMMDD_HHMMSS.txt.pgp
              e.g. PPS_Project_team_20260127_143000.txt.pgp
        - Project import:      <project_prefix><timestamp>.txt.pgp
              i.e. starts with the project prefix followed by a digit, NOT PPS_Project_team_
              e.g. PPS_Project_20260127_143000.txt.pgp

    Args:
        file_path: Full SFTP file path
        expected_prefix: Resource-assignment file name prefix (e.g. "PPS_Project_team")
        project_prefix: Project-import file name prefix (e.g. "PPS_Project_")

    Returns:
        dict: {'is_valid', 'is_project_file', 'is_unknown_file', 'error_message'}

    Note:
        - is_valid=True        -> resource-assignment file, process it
        - is_project_file=True -> project-import file, silently skip (delete dagrun,
                                  do NOT archive - leave it for the project-import DAG)
        - is_unknown_file=True -> unrecognised name, archive + notify
    """
    file_name = path.basename(file_path)

    # Resource assignment file:
    # <expected_prefix>_YYYYMMDD_HHMMSS.txt.pgp
    
    resource_pattern = rf'^{re.escape(expected_prefix)}_\d{{8}}_\d{{6}}\.txt\.pgp$'
    # Project-import file shares this inbound folder: <project_prefix><timestamp>.txt.pgp
    # (starts with the project prefix + a digit, NOT PPS_Project_team_). Leave it for the
    # project-import DAG - do NOT archive.
    project_pattern = rf'^{re.escape(project_prefix)}\d.*\.txt\.pgp$'

    if re.match(resource_pattern, file_name):
        return {
            'is_valid': True,
            'is_project_file': False,
            'is_unknown_file': False,
            'error_message': ''
        }
    elif re.match(project_pattern, file_name):
        return {
            'is_valid': False,
            'is_project_file': True,
            'is_unknown_file': False,
            'error_message': f"File is a PeopleSoft project-import file: {file_name}. "
                             f"This will be processed by the project import DAG."
        }
    else:
        return {
            'is_valid': False,
            'is_project_file': False,
            'is_unknown_file': True,
            'error_message': f"Invalid file name: {file_name}. "
                             f"Expected resource assignment format: {expected_prefix}_YYYYMMDD_HHMMSS.txt.pgp"   
        }

mandatory_fields = {
    "employee_id": "employee_id",
    "project_id": "project_id"
}

def get_invalid_logs_property_conf(item):
    """Generate log properties for validation errors"""
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "employee_id": item.get('employee_id', ''),
        "project_id": item.get('project_id', ''),
        'action': 'Validation',
        "details": get_missing_field() + " not present in feed file",
        "status": 'Exception'
    }


def get_user_not_found_log_properties(item):
    """Generate log properties for user not found errors"""
    return {
        "employee_id": item.get('employee_id', ''),
        "project_id": item.get('project_id', ''),
        'action': 'Validation',
        "details": f"User {item.get('employee_id')} not found or inactive in Replicon",
        "status": 'Exception'
    }


CSV_FIELDS = (
    'project_id',
    'employee_id',
    'employee_name',
    'assignmentstartdate',
    'assignmentenddate',
)


def normalize_csv_fields(records):
    """Coerce None values in CSV-derived fields to '' so downstream
    .strip()/.upper() calls are safe regardless of trailing-pipe variations."""
    for record in records:
        for field in CSV_FIELDS:
            if record.get(field) is None:
                record[field] = ''
    return records


def get_project_dates():
    """
    Extract project start and end dates from project details

    Returns:
        tuple: (project_start_datetime, project_end_datetime) or (None, None) if dates unavailable
    """
    project_details = rail.result("get_project_details")
    date_range = project_details.get('timeEntryDateRange', {})

    start_date_obj = date_range.get('startDate', {})
    end_date_obj = date_range.get('endDate', {})

    if start_date_obj and end_date_obj:
        try:
            project_start = datetime(
                year=start_date_obj['year'],
                month=start_date_obj['month'],
                day=start_date_obj['day']
            )
            project_end = datetime(
                year=end_date_obj['year'],
                month=end_date_obj['month'],
                day=end_date_obj['day']
            )
            return project_start, project_end
        except (KeyError, TypeError, ValueError):
            return None, None

    return None, None


def determine_assignment_dates(record, project_start, project_end,
                               user_start, user_end, DATE_FORMAT_INPUT):
    """
    Determine final assignment dates based on Guidehouse business logic.

    For dates NOT provided in the feed, fall back to the intersection of the
    project window and the user's employment window. This ensures users who
    joined after the project started (or left before it ends) still get an
    allocation covering only the portion they actually overlap.

    Implicit (missing) fallbacks:
      - missing start -> max(project_start, user_start) if user_start else project_start
      - missing end   -> min(project_end,   user_end)   if user_end   else project_end

    Explicit feed dates are used as-is; if they fall outside the user window,
    Validation 3 in the caller still skips the record with an exception so
    feed-quality issues remain visible.

    Args:
        record: Assignment record with assignmentstartdate/assignmentenddate
        project_start: Project start datetime
        project_end: Project end datetime
        user_start: Parsed user employment start datetime (may be None)
        user_end: Parsed user employment end datetime (may be None)
        DATE_FORMAT_INPUT: Date format string (e.g., "%Y-%m-%d")

    Returns:
        tuple: (final_start_datetime, final_end_datetime, error_message)
               error_message is None if successful
    """
    # Get raw date strings
    csv_start_date = record.get('assignmentstartdate', '').strip()
    csv_end_date = record.get('assignmentenddate', '').strip()

    # Parse provided dates
    provided_start = None
    provided_end = None

    if csv_start_date:
        try:
            provided_start = datetime.strptime(csv_start_date, DATE_FORMAT_INPUT)
        except (ValueError, AttributeError):
            return None, None, f"Invalid start date format: '{csv_start_date}' Expected: {DATE_FORMAT_INPUT}"

    if csv_end_date:
        try:
            provided_end = datetime.strptime(csv_end_date, DATE_FORMAT_INPUT)
        except (ValueError, AttributeError):
            return None, None, f"Invalid end date format: '{csv_end_date}' Expected: {DATE_FORMAT_INPUT}"

    default_start = max(project_start, user_start) if user_start else project_start
    default_end = min(project_end, user_end) if user_end else project_end

    final_start = provided_start or default_start
    final_end = provided_end or default_end

    return final_start, final_end, None


def validate_and_consolidate_assignment_records():
    """
    Load and validate ALL assignment records for the project

    Polaris pattern: Process all resources for a project at once using bulk API

    Business Logic:
    - Load all records for the project
    - Each record represents a resource to assign
    - Return all records for bulk processing

    Returns:
        list: All assignment records for the project
    """
    all_records = normalize_csv_fields(
        rail.load_all_records(rail.result("get_assignment_data_from_query"))
    )

    if not all_records or len(all_records) == 0:
        raise ValueError("No assignment records found for this project")

    return all_records


def prepare_resources_for_processing(DATE_FORMAT_INPUT):
    """
    Prepare resources and generate logs for processing

    Assignment Date Business Logic:
    Case 1: No start & no end date -> Use entire project duration
    Case 2: Start provided, End NOT provided -> Use provided start + project end
    Case 3: End provided, Start NOT provided -> Use project start + provided end
    Case 4: Both provided -> Use both provided dates

    Validates:
    1. Date format validation (YYYY-MM-DD)
    2. Start date must be before or equal to end date
    3. Assignment dates must fall within project start/end dates
    4. Assignment dates must fall within user's employment dates (if provided)

    Expected Fields in Record:
    - assignmentstartdate: Assignment start date (YYYY-MM-DD) [Optional]
    - assignmentenddate: Assignment end date (YYYY-MM-DD) [Optional]
    - userstartdate: User employment start date (YYYY-MM-DD) [Optional]
    - userenddate: User employment end date (YYYY-MM-DD) [Optional]

    Args:
        DATE_FORMAT_INPUT: Date format string (e.g., "%Y-%m-%d")

    Returns:
        dict: {
            'resources_to_add': list of new resources to create,
            'resources_to_update': list of existing resources to update,
            'all_logs': list of log entries for all resources
        }
    """
    all_records = normalize_csv_fields(rail.result("load_all_assignment_records"))
    existing_allocations = rail.result("get_existing_resource_allocations")  # GraphQL result
    project_details = rail.result("get_project_details")

    resources_to_add = []
    resources_to_update = []
    all_logs = []

    def get_date_from_replicon_date(replicon_date):
        if not replicon_date:
            return None
        return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year']).strftime("%Y-%m-%d")

    # Get project dates once per batch using helper function
    project_start_dt, project_end_dt = get_project_dates()
    if not project_start_dt or not project_end_dt:
        # If we can't get project dates, log error for all records
        for record in all_records:
            all_logs.append({
                'employee_id': record.get('employee_id', ''),
                'project_id': record.get('project_id', ''),
                'action': 'Validation',
                'status': 'Exception',
                'details': "Could not retrieve project start/end dates"
            })
        return {
            'resources_to_add': [],
            'resources_to_update': [],
            'all_logs': all_logs
        }

    for record in all_records:
        user_uri = record['user_uri']
        employee_id = record.get('employee_id', '')
        project_id = record.get('project_id', '')

        # Get user dates from record (if provided)
        user_start_date = record.get('userstartdate', '').strip() if record.get('userstartdate') else None
        user_end_date = record.get('userenddate', '').strip() if record.get('userenddate') else None

        # Parse user dates first so we can pass them to determine_assignment_dates
        # (they are used to clamp missing feed dates to the user's employment window).
        user_start_parsed = None
        user_end_parsed = None

        if user_start_date:
            try:
                user_start_parsed = datetime.strptime(user_start_date, DATE_FORMAT_INPUT)
            except (ValueError, AttributeError) as e:
                all_logs.append({
                    'employee_id': employee_id,
                    'project_id': project_id,
                    'action': 'Validation',
                    'status': 'Exception',
                    'details': f"Invalid user start date format: '{user_start_date}' Expected: {DATE_FORMAT_INPUT} Error: {str(e)}"
                })
                continue

        if user_end_date:
            try:
                user_end_parsed = datetime.strptime(user_end_date, DATE_FORMAT_INPUT)
            except (ValueError, AttributeError) as e:
                all_logs.append({
                    'employee_id': employee_id,
                    'project_id': project_id,
                    'action': 'Validation',
                    'status': 'Exception',
                    'details': f"Invalid user end date format: '{user_end_date}' Expected: {DATE_FORMAT_INPUT} Error: {str(e)}"
                })
                continue

        # Apply Guidehouse assignment date logic
        final_start, final_end, error_msg = determine_assignment_dates(
            record, project_start_dt, project_end_dt,
            user_start_parsed, user_end_parsed, DATE_FORMAT_INPUT
        )

        if error_msg:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': error_msg
            })
            continue

        # Write resolved dates back so downstream payload functions always get a valid string
        record['assignmentstartdate'] = final_start.strftime(DATE_FORMAT_INPUT)
        record['assignmentenddate'] = final_end.strftime(DATE_FORMAT_INPUT)

        # Continue with existing validation logic using final_start, final_end
        csv_start_parsed = final_start
        csv_end_parsed = final_end

        # Validation 1: Start date must be before or equal to end date
        if csv_start_parsed > csv_end_parsed:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is after end date"
            })
            continue

        # Validation 2: Assignment dates must be within project dates
        date_validation_failed = False

        if project_start_dt and csv_start_parsed < project_start_dt:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is before project start date"
            })
            date_validation_failed = True

        if project_start_dt and csv_end_parsed < project_start_dt:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment end date is before project start date"
            })
            date_validation_failed = True

        if project_end_dt and csv_start_parsed > project_end_dt:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is after project end date"
            })
            date_validation_failed = True

        if project_end_dt and csv_end_parsed > project_end_dt:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment end date is after project end date"
            })
            date_validation_failed = True

        # Skip this record if date validation failed
        if date_validation_failed:
            continue

        # Validation 3: Assignment dates must be within user's employment dates
        user_date_validation_failed = False

        if user_start_parsed and csv_start_parsed < user_start_parsed:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is before user start date"
            })
            user_date_validation_failed = True

        if user_start_parsed and csv_end_parsed < user_start_parsed:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment end date is before user start date"
            })
            user_date_validation_failed = True

        if user_end_parsed and csv_start_parsed > user_end_parsed:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is after user end date"
            })
            user_date_validation_failed = True

        if user_end_parsed and csv_end_parsed > user_end_parsed:
            all_logs.append({
                'employee_id': employee_id,
                'project_id': project_id,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment end date is after user end date"
            })
            user_date_validation_failed = True

        # Skip this record if user date validation failed
        if user_date_validation_failed:
            continue

        # Check if resource already has allocation
        existing_allocation = existing_allocations.get(user_uri)

        if not existing_allocation:
            # New resource - will be created
            resources_to_add.append(record)
            all_logs.append({
                'employee_id': record.get('employee_id', ''),
                'project_id': record.get('project_id', ''),
                'action': 'Add',
                'status': 'Success',
                'details': "Resource assignment added successfully"
            })
        else:
            # Resource exists - check if dates match existing allocation
            existing_start_date = existing_allocation.get('startDate', '')
            existing_end_date = existing_allocation.get('endDate', '')

            dates_match = False

            if existing_start_date and existing_end_date:
                try:
                    # Parse existing dates (ISO format)
                    existing_start_parsed = datetime.fromisoformat(existing_start_date.replace('Z', '+00:00'))
                    existing_end_parsed = datetime.fromisoformat(existing_end_date.replace('Z', '+00:00'))
                    csv_start_dt = csv_start_parsed.replace(tzinfo=existing_start_parsed.tzinfo)
                    csv_end_dt = csv_end_parsed.replace(tzinfo=existing_end_parsed.tzinfo)

                    # Check if both start and end dates match
                    if existing_start_parsed.date() == csv_start_dt.date() and existing_end_parsed.date() == csv_end_dt.date():
                        dates_match = True
                except (ValueError, AttributeError):
                    all_logs.append({
                        'employee_id': record.get('employee_id', ''),
                        'project_id': record.get('project_id', ''),
                        'action': 'Update',
                        'status': 'Exception',
                        'details': "Resource assignment is skipped, invalid existing date format in Replicon"
                    })
                    continue

            if dates_match:
                # Dates match exactly - skip update
                all_logs.append({
                    'employee_id': record.get('employee_id', ''),
                    'project_id': record.get('project_id', ''),
                    'action': 'Skip',
                    'status': 'Exception',
                    'details': "Resource assignment is skipped, due to no changes in assignment dates"
                })
            else:
                record['existing_allocation_id'] = existing_allocation.get('allocation_id', '')
                resources_to_update.append(record)

                all_logs.append({
                        'employee_id': record.get('employee_id', ''),
                        'project_id': record.get('project_id', ''),
                        'action': 'Update',
                        'status': 'Success',
                        'details': "Resource assignment updated successfully"
                    })

    return {
        'resources_to_add': resources_to_add,
        'resources_to_update': resources_to_update,
        'all_logs': all_logs
    }


def format_logs_callable():
    """
    Format and aggregate logs from all DAG runs
    Calculates success, error, and exception counts for email reporting
    """
    final_log_records = rail.load_all_records(rail.result("create_exception_log"))

    # Set counters for email template
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'exception', final_log_records))))

    return rail.write_json_artifact(final_log_records)
