from datetime import datetime
import re
from os import path
import rail

def validate_assignment_file_name(file_path, expected_prefix):
    """
    Validate resource assignment file name matches expected pattern
    If file doesn't match, it could be a project file (which is handled by project DAG)

    Pattern: {PREFIX}_Assignment_YYYYMMDD_HHMMSS.csv.pgp
    Examples:
        - DEV_Assignment_20250127_143022.csv.pgp
        - UAT_Assignment_20250127_143022.csv.pgp
        - PROD_Assignment_20250127_143022.csv.pgp

    Args:
        file_path: Full SFTP file path
        expected_prefix: Environment prefix (DEV, UAT, PROD)

    Returns:
        dict: {'is_valid': bool}

    Note:
        - If is_valid=True: This is a resource assignment file, process it
        - If is_valid=False: This is NOT a resource assignment file (could be project file),
          silently skip by deleting dagrun - no error emails needed
    """
    file_name = path.basename(file_path)

    # Pattern: {PREFIX}_Assignment_YYYYMMDD_HHMMSS.csv.pgp
    pattern = rf'^{expected_prefix}_Assignment_\d{{8}}_\d{{6}}\.csv\.pgp$'

    if re.match(pattern, file_name):
        return {
            'is_valid': True
        }
    else:
        # Not a resource file - could be project file, silently skip
        return {
            'is_valid': False
        }

mandatory_fields = {
    "workernumber": "workernumber",
    "projectnumber": "projectnumber",
    "assignmentstartdate": "assignmentstartdate"
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
        "workernumber": item.get('workernumber', ''),
        "projectnumber": item.get('projectnumber', ''),
        'action': 'Validation',
        "details": get_missing_field() + " not present in feed file",
        "status": 'Exception'
    }


def get_user_not_found_log_properties(item):
    """Generate log properties for user not found errors"""
    return {
        "workernumber": item.get('workernumber', ''),
        "projectnumber": item.get('projectnumber', ''),
        'action': 'Validation',
        "details": f"User {item.get('workernumber')} not found or inactive in Replicon",
        "status": 'Exception'
    }


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
    all_records = rail.load_all_records(rail.result("get_assignment_data_from_query"))

    if not all_records or len(all_records) == 0:
        raise ValueError("No assignment records found for this project")

    return all_records


def prepare_resources_for_processing(DATE_FORMAT_INPUT):
    """
    Prepare resources and generate logs for processing
    Categorizes resources based on existing allocations and creates separate lists

    Validates:
    1. Date format validation (DD-MMM-YYYY)
    2. Start date must be before or equal to end date
    3. Assignment dates must fall within project start/end dates
    4. Assignment dates must fall within user's employment dates (if provided)

    Expected Fields in Record:
    - assignmentstartdate: Assignment start date (DD-MMM-YYYY)
    - assignmentenddate: Assignment end date (DD-MMM-YYYY)
    - userstartdate: User employment start date (DD-MMM-YYYY) [Optional]
    - userenddate: User employment end date (DD-MMM-YYYY) [Optional]

    Args:
        DATE_FORMAT_INPUT: Date format string (e.g., "%d-%b-%Y")

    Returns:
        dict: {
            'resources_to_add': list of new resources to create,
            'resources_to_update': list of existing resources to update,
            'all_logs': list of log entries for all resources
        }
    """
    all_records = rail.result("load_all_assignment_records")
    existing_allocations = rail.result("get_existing_resource_allocations")  # GraphQL result
    project_details = rail.result("get_project_details")

    resources_to_add = []
    resources_to_update = []
    all_logs = []

    def get_date_from_replicon_date(replicon_date):
        if not replicon_date:
            return None
        return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year']).strftime("%Y-%m-%d")

    # Get project dates for validation
    project_start_date = project_details.get('timeEntryDateRange',{}).get("startDate")
    project_end_date = project_details.get('timeEntryDateRange',{}).get('endDate')

    # Parse project dates (format: YYYY-MM-DD)
    project_start_dt = None
    project_end_dt = None
    if project_start_date:
        project_start_str = get_date_from_replicon_date(project_start_date)
        if project_start_str:
            project_start_dt = datetime.strptime(project_start_str, '%Y-%m-%d')
    if project_end_date:
        project_end_str = get_date_from_replicon_date(project_end_date)
        if project_end_str:
            project_end_dt = datetime.strptime(project_end_str, '%Y-%m-%d')

    for record in all_records:
        user_uri = record['user_uri']
        csv_start_date = record.get('assignmentstartdate', '').strip().upper()
        csv_end_date = record.get('assignmentenddate', '').strip().upper()
        workernumber = record.get('workernumber', '')
        projectnumber = record.get('projectnumber', '')

        # Get user dates from record (if provided)
        user_start_date = record.get('userstartdate', '').strip().upper() if record.get('userstartdate') else None
        user_end_date = record.get('userenddate', '').strip().upper() if record.get('userenddate') else None

        # Parse CSV dates
        try:
            csv_start_parsed = datetime.strptime(csv_start_date, DATE_FORMAT_INPUT)
            csv_end_parsed = datetime.strptime(csv_end_date, DATE_FORMAT_INPUT)
        except (ValueError, AttributeError) as e:
            # Invalid date format
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, invalid date format received for assignment dates"
            })
            continue

        # Parse user dates if provided
        user_start_parsed = None
        user_end_parsed = None

        if user_start_date:
            try:
                user_start_parsed = datetime.strptime(user_start_date, DATE_FORMAT_INPUT)
            except (ValueError, AttributeError):
                all_logs.append({
                    'workernumber': workernumber,
                    'projectnumber': projectnumber,
                    'action': 'Validation',
                    'status': 'Exception',
                    'details': "Resource assignment is skipped, invalid user start date format received"
                })
                continue

        if user_end_date:
            try:
                user_end_parsed = datetime.strptime(user_end_date, DATE_FORMAT_INPUT)
            except (ValueError, AttributeError):
                all_logs.append({
                    'workernumber': workernumber,
                    'projectnumber': projectnumber,
                    'action': 'Validation',
                    'status': 'Exception',
                    'details': "Resource assignment is skipped, invalid user end date format received"
                })
                continue

        # Validation 1: Start date must be before or equal to end date
        if csv_start_parsed > csv_end_parsed:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is after end date"
            })
            continue

        # Validation 2: Assignment dates must be within project dates
        date_validation_failed = False

        if project_start_dt and csv_start_parsed < project_start_dt:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is before project start date"
            })
            date_validation_failed = True

        if project_start_dt and csv_end_parsed < project_start_dt:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment end date is before project start date"
            })
            date_validation_failed = True

        if project_end_dt and csv_start_parsed > project_end_dt:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is after project end date"
            })
            date_validation_failed = True

        if project_end_dt and csv_end_parsed > project_end_dt:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
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
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is before user start date"
            })
            user_date_validation_failed = True

        if user_start_parsed and csv_end_parsed < user_start_parsed:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment end date is before user start date"
            })
            user_date_validation_failed = True

        if user_end_parsed and csv_start_parsed > user_end_parsed:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
                'action': 'Validation',
                'status': 'Exception',
                'details': "Resource assignment is skipped, assignment start date is after user end date"
            })
            user_date_validation_failed = True

        if user_end_parsed and csv_end_parsed > user_end_parsed:
            all_logs.append({
                'workernumber': workernumber,
                'projectnumber': projectnumber,
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
                'workernumber': record.get('workernumber', ''),
                'projectnumber': record.get('projectnumber', ''),
                'action': 'Add',
                'status': 'Success',
                'details': "Resource assignment added successfully"
            })
        else:
            # Resource exists - check if dates match existing allocation
            existing_start_date = existing_allocation.get('startDate', '')
            existing_end_date = existing_allocation.get('endDate', '')

            dates_match = False
            start_date_mismatch = False

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
                    elif existing_start_parsed.date() != csv_start_dt.date():
                        start_date_mismatch = True
                        # When start dates differ, we retain existing start date
                        # Check if the RESULT (existing start + CSV end) already matches what's in Replicon
                        if existing_end_parsed.date() == csv_end_dt.date():
                            # The final result would be the same as what's already in Replicon
                            dates_match = True
                except (ValueError, AttributeError):
                    all_logs.append({
                        'workernumber': record.get('workernumber', ''),
                        'projectnumber': record.get('projectnumber', ''),
                        'action': 'Update',
                        'status': 'Exception',
                        'details': "Resource assignment is skipped, invalid existing date format in Replicon"
                    })
                    continue

            if dates_match:
                # Dates match exactly - skip update
                all_logs.append({
                    'workernumber': record.get('workernumber', ''),
                    'projectnumber': record.get('projectnumber', ''),
                    'action': 'Skip',
                    'status': 'Exception',
                    'details': "Resource assignment is skipped, due to no changes in assignment dates"
                })
            else:
                # Dates don't match - will be updated
                # Store existing start date to retain it if there's a mismatch
                record['existing_start_date'] = existing_start_date
                record['existing_allocation_id'] = existing_allocation.get('allocation_id', '')

                resources_to_update.append(record)

                if start_date_mismatch:
                    # Start date mismatch - log exception
                    all_logs.append({
                        'workernumber': record.get('workernumber', ''),
                        'projectnumber': record.get('projectnumber', ''),
                        'action': 'Update',
                        'status': 'Exception',
                        'details': "Start date for resource assignment skipped but end date is updated"
                    })
                else:
                    # Only end date changed - normal update
                    all_logs.append({
                        'workernumber': record.get('workernumber', ''),
                        'projectnumber': record.get('projectnumber', ''),
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
