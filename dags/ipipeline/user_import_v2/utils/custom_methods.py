"""
CHANGE LOG - ACCRUAL CONDITIONS FILTERING & SCHEDULED HOURS SUPPORT
Date: 2025-01-21
Issue: Time off types with accrual_conditions: null were being included in timeoff policy updates

CHANGES MADE:
1. build_comprehensive_timeoff_assignments_for_update(): Added filtering to exclude timeoff types without accrual conditions
   - See CHECKPOINT: ACCRUAL_CONDITIONS_FILTERING_START/END
   - See CHECKPOINT: NEW_TIMEOFF_FILTERING_START/END  
   - See CHECKPOINT: OVERLAPPING_TIMEOFF_FILTERING_START/END

2. Added support for scheduled hours changes (UKG integration):
   - New policy lines created automatically when scheduled hours change
   - Same logic as FTE changes - preserves existing policies + adds new policy line
   - Combined condition: if fte_changed or schedule_changed

BUG FIX:
- Fixed field name matching in filtering logic: policy.get("name") -> policy.get("time_off_type")
- Was excluding ALL time off types due to incorrect field name lookup

REVERT INSTRUCTIONS:
To fully revert all filtering changes:
1. Replace filtered_timeoff_types logic with: filtered_timeoff_types = dag_run.conf.get("calculated_time_off_types", {})
2. Replace new timeoff loop with: for timeoff_uri in timeoff_changes.get("new_timeoff_types", []):
3. Replace overlapping lookup with: for type_name, type_config in dag_run.conf["calculated_time_off_types"].items():

To revert scheduled hours support:
1. Change: fte_changed or schedule_changed -> fte_changed
2. Remove: schedule_changed = change_details.get('is_scheduled_hours_changed', False)
"""

from functools import lru_cache
from hashlib import sha256
from datetime import datetime
import itertools
import json
import pendulum
import rail

null = None
true = True
false = False


def get_process_users_dag_ids(parallel_count):
    active_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_user_record_{x+1}') if rail.result(
            f'process_user_record_{x+1}') else []), range(parallel_count)))))

    return active_users


def process_user_row_with_hash(item):
    # Step 1: Get mandatory client-provided HASH and create SHA256 from it
    # Fields are already mapped, so use internal field name "hash_value"
    client_hash = item.get('hash_value')

    # Create SHA256 hash from the client-provided hash for secure comparison
    sha256_hash = sha256(client_hash.encode('utf-8')).hexdigest()

    # Step 2: Return list of values in the same order as header
    return [
        item.get('employee_id'),
        item.get('first_name'),
        item.get('last_name'),
        item.get('display_name'),
        item.get('email'),
        item.get('start_date'),
        item.get('end_date'),
        item.get('login_name'),
        item.get('authentication_type'),
        item.get('authentication_id'),
        item.get('supervisor'),
        item.get('language'),
        item.get('fte'),
        item.get('level'),
        item.get('title'),
        item.get('location_level_1'),
        item.get('location_level_2'),
        item.get('employee_schedule'),
        item.get('department_level_1'),
        item.get('department_level_2'),
        item.get('employee_category'),
        item.get('scheduled_hours'),
        item.get('elt'),
        item.get('uksick'),
        item.get('transfer_date'),
        item.get('employee_type'),
        item.get('paygroup'),
        item.get('project'),
        item.get('seniority_level'),
        item.get('hash_value'),
        sha256_hash
    ]


def get_input_columns(input_fields_mapper_data):
    """
    Extract field_name from input fields mapper for collection table mapping

    Args:
        input_fields_mapper_data: List of field mapping dictionaries from INPUT_FIELDS

    Returns:
        Dict mapping csv_field to field_name for collection operations
    """
    return {field["csv_field"]: field["field_name"] for field in input_fields_mapper_data}


def get_invalid_user_input_details(dag_run, is_update, input_fields_mapper_data):
    """
    Get details about invalid user input for processing.

    Args:
        dag_run: Airflow DagRun object containing configuration
        is_update: Boolean indicating if this is an update operation
        input_fields_mapper_data: List of field mapping dictionaries

    Returns:
        str: Error message describing missing mandatory fields, or empty string if all fields present
    """
    if is_update:
        mandatory_fields = {
            field["csv_field"]: field["field_name"]
            for field in input_fields_mapper_data
            if field.get("mandatory_update", True)
        }
    else:
        mandatory_fields = {
            field["csv_field"]: field["field_name"]
            for field in input_fields_mapper_data
            if field.get("mandatory_add", True)
        }

    blank_fields = []
    for csv_field, field_name in mandatory_fields.items():
        # Check if field is missing from config or has empty/null value
        if (field_name not in dag_run.conf or
            dag_run.conf[field_name] is null or
                str(dag_run.conf[field_name]).strip() == ""):
            blank_fields.append(csv_field)

    if blank_fields:
        message = "User not processed due to following reason/s: "
        return message + "; ".join(blank_fields) + " not present in the input data"
    else:
        return null  # Return empty string when all mandatory fields are present


def create_users_payload_from_variable(item, config):
    """
    Main business logic processor - exactly like tsystems pattern
    Updated to work with hierarchical group structure using '/' separator

    Args:
        item: User record from CSV/API with calculated fields
        config: Configuration object with mapper data

    Returns:
        Processed user payload with calculated URI fields for Replicon user creation
    """
    # Apply iPipeline business logic transformations (includes URI lookups)
    merged_item = {
        **item,
        **apply_ipipeline_business_logic(item, config)
    }

    return merged_item


def find_group_uri_by_name_and_path(groups_data, name, expected_path):
    """
    Find group URI by matching both name and fullpath (to ensure correct hierarchy)
    This matches the logic used in group creation tasks

    Args:
        groups_data: List of group objects with name, fullpath, uri fields
        name: Group name to match
        expected_path: Expected fullpath (e.g., "Level1/Level2")

    Returns:
        URI string if found, null otherwise
    """
    for group in groups_data:
        if (group.get('name') == name and
                group.get('fullpath') == expected_path):
            return group.get('uri')

    return null


def apply_ipipeline_business_logic(user_data, config):
    """
    Apply iPipeline-specific business rules in sequence (following tsystems pattern)

    Per iPipeline Tech Spec Requirements:
    - Employee Type: Based on employee_category, employee_schedule, employee_type combination
    - Login Status: Always 'Enabled' for iPipeline users
    - Permissions: Based on title mapping from tech spec
    - Activities: On-call activities for specific departments/locations only
    - Timesheet Template: From assignment rules matrix
    - Time Zone: Location-based mapping
    - Time Off: Prorated accruals based on FTE and service years
    - Rate Cards: Title-based rate assignments
    - URI Lookups: Find appropriate URIs for locations, departments, employee types

    Args:
        user_data: Mapped user data with internal field names
        config: Configuration with mapper data

    Returns:
        Dictionary with calculated fields including URIs
    """
    result = {}

    # Load organizational group data for URI lookups from rail results
    # These will be available from get_updated_groups_data task results after group creation
    locations_data = rail.load_all_records(
        rail.result("get_updated_location_groups_data"))
    departments_data = rail.load_all_records(
        rail.result("get_updated_department_groups_data"))
    employeetypes_data = rail.load_all_records(
        rail.result("get_updated_employeetype_groups_data"))
    org_roles_data = rail.load_all_records(
        rail.result("get_updated_servicecenter_groups_data"))
    projectroles_data = rail.load_all_records(
        rail.result("get_updated_project_roles"))
    activities_data = rail.result("get_required_activities")
    permissions_data = rail.result("get_required_permission_sets")

    result['calculated_orgrole_data'] = rail.result(
        "get_updated_servicecenter_groups_data")

    # Step 1: Login status - Always enabled per iPipeline tech spec
    result['calculated_login_status'] = true

    # Step 3: Assignment rules - Extract all values from comprehensive mapper
    # This handles on-call vs regular employee differentiation
    assignment_rule = get_assignment_rule_from_mapper(
        user_data.get('location_level_1'),
        user_data.get('location_level_2'),
        user_data.get('department_level_1'),
        user_data.get('employee_category'),
        config.assignment_rules_mapper_data
    )

    # Extract only essential assignment rule values
    result['calculated_schedule_type'] = assignment_rule.get('schedule_type')
    result['calculated_timesheet_template'] = assignment_rule.get(
        'timesheet_template')
    result['calculated_work_week'] = assignment_rule.get('work_week')
    result['calculated_timesheet_and_time_entry_notification'] = assignment_rule.get(
        'timesheet_and_time_entry_notification')
    result['calculated_overtime_request_template'] = assignment_rule.get(
        'overtime_request_template')
    result['calculated_overtime_request_approval_path'] = assignment_rule.get(
        'overtime_request_approval_path')
    # Convert activities list to dictionary with URIs
    activities_list = assignment_rule.get('activities', [])
    calculated_activities = {}
    if activities_list and activities_data:
        for activity_name in activities_list:
            if activity_name in activities_data:
                calculated_activities[activity_name] = activities_data[activity_name]
    result['calculated_activities'] = calculated_activities

    result['calculated_payrule'] = assignment_rule.get('payrule')
    result['calculated_payrule_uri'] = rail.result("get_required_payrules").get(
        assignment_rule.get('payrule')) if assignment_rule.get('payrule') else null

    # Calculate timesheet period URI from assignment rule
    result['calculated_timesheet_period'] = assignment_rule.get(
        'timesheet_period')
    result['calculated_timesheet_period_uri'] = rail.result("get_all_timesheet_periods").get(
        assignment_rule.get('timesheet_period')) if assignment_rule.get('timesheet_period') else null

    # Calculate schedule type URI based on assignment_rules_mapper schedule_type
    # "Office Schedule" or "Shift Schedule" from assignment_rules_mapper
    mapper_schedule_type = assignment_rule.get('schedule_type')
    # From SOURCE ABC file (40 hours, 37.5 hours, etc.)
    input_schedule_name = user_data.get('scheduled_hours')

    if mapper_schedule_type == 'Shift Schedule':
        # Shift schedule - hardcoded shift URI
        result['calculated_schedule_type_uri'] = 'urn:replicon:schedule-type:shift'
        result['calculated_office_schedule_uri'] = null
    elif mapper_schedule_type == 'Office Schedule':
        # Office schedule - hardcoded office schedule type URI + lookup office schedule name from get_all_office_schedules
        result['calculated_schedule_type_uri'] = 'urn:replicon:schedule-type:office-schedule'
        result['calculated_office_schedule_uri'] = rail.result(
            "get_all_office_schedules").get(input_schedule_name) if input_schedule_name else null
    else:
        # No valid schedule type found
        result['calculated_schedule_type_uri'] = null
        result['calculated_office_schedule_uri'] = null

    result['calculated_schedule_name'] = input_schedule_name
    result['calculated_mapper_schedule_type'] = mapper_schedule_type

    # Extract approval paths and templates from assignment rule
    result['calculated_timesheet_approval_path'] = assignment_rule.get(
        'timesheet_approval_path')
    result['calculated_time_entry_approval_path'] = assignment_rule.get(
        'time_entry_approval_path')
    result['calculated_time_off_template'] = assignment_rule.get(
        'time_off_template')
    result['calculated_time_off_approval'] = assignment_rule.get(
        'time_off_approval')
    result['calculated_holiday_calendar'] = assignment_rule.get(
        'holiday_calendar')

    # Step 9: Location URI lookup - Find appropriate location URI
    location_level_1 = user_data.get('location_level_1', null)
    location_level_2 = user_data.get('location_level_2', null)

    if location_level_2:
        # Use Level 2 location if available
        location_path = f"{location_level_1}/{location_level_2}"
        location_uri = find_group_uri_by_name_and_path(
            locations_data, location_level_2, location_path)
        calculated_location_name = location_level_2
    else:
        # Use Level 1 location
        location_path = location_level_1
        location_uri = find_group_uri_by_name_and_path(
            locations_data, location_level_1, location_level_1)
        calculated_location_name = location_level_1

    result['calculated_location_uri'] = location_uri
    result['calculated_location_name'] = calculated_location_name
    result['calculated_location_path'] = location_path

    # Step 10: Department URI lookup - Find appropriate department URI
    department_level_1 = user_data.get('department_level_1', null)
    department_level_2 = user_data.get('department_level_2', null)

    defaults_mapper_data = config.defaults_mapper_data

    if department_level_2:
        # Use Level 2 department if available
        department_path = f"{defaults_mapper_data.get('root_department')}/{department_level_1}/{department_level_2}"
        department_uri = find_group_uri_by_name_and_path(
            departments_data, department_level_2, department_path)
        calculated_department_name = department_level_2
    else:
        # Use Level 1 department
        department_path = f"{defaults_mapper_data.get('root_department')}/{department_level_1}"
        department_uri = find_group_uri_by_name_and_path(
            departments_data, department_level_1, department_level_1)
        calculated_department_name = department_level_1

    result['calculated_department_uri'] = department_uri
    result['calculated_department_name'] = calculated_department_name
    result['calculated_department_path'] = department_path

    # Step 11: Employee type - Create hierarchical combination from tech spec
    # Level 1: Employee Category, Level 2: Employee Schedule, Level 3: Employee Type
    employee_category = user_data.get('employee_category', null)
    employee_schedule = user_data.get('employee_schedule', null)
    employee_type = user_data.get('employee_type', null)

    # Create combined employee type identifier for Replicon groups using / separator
    if employee_type:
        calculated_employee_type = f"{employee_category}/{employee_schedule}/{employee_type}"
        employee_type_uri = find_group_uri_by_name_and_path(
            employeetypes_data, employee_type, calculated_employee_type)
    elif employee_schedule:
        calculated_employee_type = f"{employee_category}/{employee_schedule}"
        employee_type_uri = find_group_uri_by_name_and_path(
            employeetypes_data, employee_schedule, calculated_employee_type)
    else:
        calculated_employee_type = employee_category
        employee_type_uri = find_group_uri_by_name_and_path(
            employeetypes_data, employee_category, calculated_employee_type)

    result['calculated_employee_type'] = calculated_employee_type
    result['calculated_employee_type_uri'] = employee_type_uri

    # Step 12: Rate card assignment - Find project role data by title match
    # Rate cards are derived directly from project roles, not from mapper
    project_role_data = find_project_role_data_by_title(
        user_data.get('title'),
        projectroles_data
    )
    result['calculated_project_role_uri'] = project_role_data.get(
        'uri') if project_role_data else null
    result['calculated_project_role_data'] = project_role_data

    # Step 13: Org Role/Service Center data and permissions lookup
    # Find org role URI by matching user title to org role name
    user_title = user_data.get('title', null)

    # Find org role details using rail helper function with fallback to default
    org_role_details = (
        rail.find_first_by_attr_and_get_attr(org_roles_data, 'name', user_title) or
        rail.find_first_by_attr_and_get_attr(org_roles_data, 'name', 'Default')
    ) if user_title and org_roles_data else null

    # Extract org role fields from the matched org role
    org_role_uri = org_role_details.get('uri') if org_role_details else null
    org_role_code = org_role_details.get('code') if org_role_details else null

    # Get permissions based on org role code/name from permissions_mapper with URIs resolved
    org_role_name = org_role_details.get('name') if org_role_details else null
    user_permissions = get_permissions_by_orgrole_code(
        org_role_code, permissions_data, config.permissions_mapper_data, defaults_mapper_data)
    supervisor_uri = permissions_data.get(defaults_mapper_data.get(
        "supervisor_permission")) if permissions_data else null
    result['calculated_supervisor_permission'] = supervisor_uri

    result['calculated_permissions'] = user_permissions
    result['calculated_orgrole_uri'] = org_role_uri
    result['calculated_orgrole_code'] = org_role_code

    # Step 13: Holiday calendar URI lookup
    holiday_calendar = result.get('calculated_holiday_calendar')
    result['calculated_holiday_calendar_uri'] = rail.result(
        "get_required_holiday_calendars").get(holiday_calendar) if holiday_calendar else null

    # Step 14: Timesheet template URI lookup
    timesheet_template = result.get('calculated_timesheet_template')
    result['calculated_timesheet_template_uri'] = rail.result(
        "get_required_policy_set_uris").get(timesheet_template) if timesheet_template else null

    # Step 15: Overtime request template URI lookup
    overtime_request_template = result.get(
        'calculated_overtime_request_template')
    result['overtime_request_template_uri'] = rail.result("get_required_policy_set_uris").get(
        overtime_request_template) if overtime_request_template else null

    # Step 16: Overtime request approval path URI lookup
    overtime_request_approval_path = result.get(
        'calculated_overtime_request_approval_path')
    result['overtime_request_approval_path_uri'] = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_all_ot_request_approval_paths"), 'displayText', overtime_request_approval_path, 'uri') if overtime_request_approval_path else null

    return result


def get_assignment_rule_from_mapper(location_level_1, location_level_2, department_level_1, employee_category, assignment_rules_mapper_data):
    """
    Get complete assignment rule from comprehensive mapper based on tech spec requirements.
    Extracts: Schedule Type, Timesheet Template, Time Off Template, Holiday Calendar, Notifications
    Based on matching: Location L1 (with exclude support), Location L2, Department L1, Employee Category, OT TEMPLATE, OT APPROVAL PATH
    CONTRACTOR SUPPORT:
    - Contractors are matched via location_level_1 = "Contractor"
    - All other rules exclude contractors via location_level_1_to_exclude = ["Contractor"]
    - Contractor rule must be first in mapper for priority matching
    Args:
        location_level_1: Country (US, UK, Canada, Japan) or "Contractor"
        location_level_2: City/Province (or 'All')
        department_level_1: Department (Pro Services, Support, Research & Development, etc.)
        employee_category: Category (Hourly, Salaried, All)
        assignment_rules_mapper_data: List of assignment rules from config
    Returns:
        Dict with schedule_type, timesheet_template, time_off_template, holiday_calendar, notifications
    """
    if not assignment_rules_mapper_data:
        return {}

    def matches_include_exclude_lists(include_list, exclude_list, actual_value):
        """Handle include/exclude list matching logic"""
        if not actual_value:
            return False

        actual_val = str(actual_value).strip()

        # If include list is provided and not empty, value must be in include list
        if include_list and len(include_list) > 0:
            return actual_val in include_list

        # If exclude list is provided and not empty, value must NOT be in exclude list
        if exclude_list and len(exclude_list) > 0:
            return actual_val not in exclude_list

        # If both lists are empty, match all
        return True

    # Find matching rule - process from most specific to general
    for rule in assignment_rules_mapper_data:
        # Match Location Level 1 (exact match required, with exclude list support)
        location1_match = (rule.get('location_level_1') == location_level_1)
        # Check if this location_level_1 is excluded by the rule
        location1_excluded = location_level_1 in rule.get(
            'location_level_1_to_exclude', [])
        if location1_excluded:
            continue  # Skip this rule if location is excluded

        # Match Location Level 2 (using include/exclude lists)
        location2_match = matches_include_exclude_lists(
            rule.get('location_level_2_to_include', []),
            rule.get('location_level_2_to_exclude', []),
            location_level_2
        )

        # Match Department Level 1 (using include/exclude lists)
        department_match = matches_include_exclude_lists(
            rule.get('department_1_to_include', []),
            rule.get('department_1_to_exclude', []),
            department_level_1
        )

        # Match Employee Category (using include/exclude lists)
        category_match = matches_include_exclude_lists(
            rule.get('employee_category_to_include', []),
            rule.get('employee_category_to_exclude', []),
            employee_category
        )

        if location1_match and location2_match and department_match and category_match:
            # Activities are already stored as lists in the mapper
            activities = rule.get('activities', [])

            return {
                'schedule_type': rule.get('schedule_type'),
                'timesheet_template': rule.get('timesheet_template'),
                'time_off_template': rule.get('time_off_template'),
                'holiday_calendar': rule.get('holiday_calendar'),
                'timesheet_and_time_entry_notification': rule.get('timesheet_and_time_entry_notification'),
                'activities': activities,
                'payrule': rule.get('payrule'),
                'timesheet_period': rule.get('timesheet_period'),
                'work_week': rule.get('work_week'),
                'time_entry_approval_path': rule.get('time_entry_approval_path'),
                'timesheet_approval_path': rule.get('timesheet_approval_path'),
                'time_off_approval': rule.get('time_off_approval'),
                'overtime_request_template': rule.get('overtime_request_template'),
                'overtime_request_approval_path': rule.get('overtime_request_approval_path')
            }

    return {}


def find_project_role_data_by_title(title, projectroles_data):
    """
    Find complete project role data by matching user title to project role name.
    Rate cards are derived from project roles, not from separate mappers.

    Args:
        title: User's job title
        projectroles_data: List of project role objects with name, uri, billing_rate, cost_amount, etc.

    Returns:
        Complete project role data dict if found, null otherwise
    """
    if not title or not projectroles_data:
        return null

    # Try exact title match against project role names
    for role in projectroles_data:
        if role.get('name', null).lower() == title.lower():
            return role

    # No match found - return null
    return null


def get_permissions_by_orgrole_code(org_role_code, permissions_data, permissions_mapper_data, defaults_mapper_data):
    """
    Get permissions based on org role code/name from permissions_mapper and resolve URIs.

    Args:
        org_role_code: Org role code
        org_role_name: Org role name (should match user title for specific permissions)
        user_title: User's original title
        permissions_mapper_data: Permissions mapper data (dictionary with title as key)

    Returns:
        Dictionary of permission names to URIs
    """
    # Check if this is the default org role assignment
    if org_role_code == defaults_mapper_data.get('default_org_role'):
        # For default org role, assign only basic "Project Resource with Reports" permission
        permissions_list = [defaults_mapper_data.get('default_permission')]
    else:
        # For specific org roles, use the title-based permissions from mapper
        permissions_list = permissions_mapper_data.get(org_role_code, [])

    # Build permissions dictionary with URIs
    calculated_permissions = {}
    for permission_name in permissions_list:
        permission_uri = permissions_data.get(
            permission_name) if permissions_data else null
        calculated_permissions[permission_name] = permission_uri

    return calculated_permissions


@lru_cache(maxsize=16)
def get_required_timeoff_data_from_artifact(timeoff_type_data_artifact):
    return json.loads(rail.read_artifact(timeoff_type_data_artifact))

# Old complex time off function removed - now using simplified mapper from tech spec


def get_timeoff_types_for_assignment(user_data, config):
    """
    Calculate time off type assignments for master DAG.
    Policy schedules will be calculated later in get_user_creation_payload.

    Business Logic:
    1. Time off assignment based on Country, Location, Level fields and UKSICK category on User profile
    2. Visibility Control:
       - visible_to_employees=True: Auto-assigned through integration
       - visible_to_employees=False: Created but disabled, HR manages manually
    3. CONTRACTORS receive NO time off types (excluded via location_level_1_to_exclude)
    Args:
        user_data: User data with location, employee level
        config: Configuration with time_off_type_mapper_data
    Returns:
        Dictionary with calculated_time_off_types only (empty for contractors)
    """
    result = {}

    # Get timeoff types data from Replicon
    timeoff_types_data = get_required_timeoff_data_from_artifact(
        user_data['required_timeoff_types_data_artifact'])
    if not timeoff_types_data or not config.time_off_type_mapper_data:
        return {}

    # Extract user criteria for matching
    user_location_1 = user_data.get('location_level_1', null).upper()
    user_location_2 = user_data.get('location_level_2', null)
    user_employee_level = user_data.get('level', null)
    user_uksick_category = user_data.get('profile_uksick', null)

    # Results container
    # Auto-assigned timeoff types (visible_to_employees=True)
    calculated_time_off_types = {}

    for policy in config.time_off_type_mapper_data:
        # Extract policy criteria
        location_level_1 = policy.get('location_level_1', null).upper()
        location_level_1_to_exclude = policy.get(
            'location_level_1_to_exclude', [])
        location_level_2_to_include = policy.get(
            'location_level_2_to_include', [])
        location_level_2_to_exclude = policy.get(
            'location_level_2_to_exclude', [])
        employee_levels_to_include = policy.get(
            'employee_levels_to_include', [])
        employee_levels_to_exclude = policy.get(
            'employee_levels_to_exclude', [])
        uksick_category_to_include = policy.get(
            'uksick_category_to_include', [])
        uksick_category_to_exclude = policy.get(
            'uksick_category_to_exclude', [])
        timeoff_type_name = policy.get('time_off_type', null)

        # Skip if timeoff type not available in Replicon
        if not timeoff_type_name or timeoff_type_name not in timeoff_types_data:
            continue

        # Check if user's location_level_1 is excluded from this policy
        if location_level_1_to_exclude and user_location_1 in [loc.upper() for loc in location_level_1_to_exclude]:
            # Skip this time off type (contractors will be excluded here)
            continue

        # Match location level 1 (ALL or exact match)
        if location_level_1 != 'ALL' and location_level_1 != user_location_1:
            continue

        # Match location level 2 using include/exclude logic
        if location_level_2_to_include and user_location_2 not in location_level_2_to_include:
            continue
        if location_level_2_to_exclude and user_location_2 in location_level_2_to_exclude:
            continue

        # Match employee levels using include/exclude logic
        if employee_levels_to_include and user_employee_level not in employee_levels_to_include:
            continue
        if employee_levels_to_exclude and user_employee_level in employee_levels_to_exclude:
            continue

        # Match UKSICK using include/exclude logic
        if uksick_category_to_include and user_uksick_category not in uksick_category_to_include:
            continue
        if uksick_category_to_exclude and user_uksick_category in uksick_category_to_exclude:
            continue

        # Policy matches user criteria - get timeoff URI and configuration
        timeoff_uri = timeoff_types_data[timeoff_type_name]

        calculated_time_off_types[timeoff_type_name] = {
            "uri": timeoff_uri,
            "reference_logic_type": policy.get("reference_logic_type"),
            "visible_to_employees": policy.get("visible_to_employees"),
            "timeoff_type_name": timeoff_type_name
        }

    return calculated_time_off_types


def logging_details(time_zone, STANDARD_EMAIL_DATE_FORMAT, YMD_DATE_FORMAT):
    """
    Log import start details
    """
    today = pendulum.now(time_zone)
    file_name = rail.render_template(
        "{{ result('new_file_sensor') | file_base }}")
    return {
        "current_date": today.strftime(YMD_DATE_FORMAT),
        "process_start_time": today.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_filename": f'Logs_{file_name}_{today.strftime("%Y%m%dT%H%M%S")}.csv',
        "time_zone": time_zone
    }


def get_error_message():
    """
    Get current error message from task context
    """
    try:
        # This would typically get the error from Airflow context
        return rail.get_current_context().get('exception', 'Unknown error occurred')
    except:
        return 'Unknown error occurred'


def do_format_logs():
    log_artifacts = []
    log_records = []

    # Get user logs and groups logs
    logs = (rail.result("gather_user_logs") if rail.result(
        "gather_user_logs") else []) + [rail.result("create_groups_log")]

    # Add supervisor assignment logs if they exist
    supervisor_assignment_log = rail.result("create_supervisor_assignment_log") if rail.result(
        "create_supervisor_assignment_log") else null
    if supervisor_assignment_log:
        logs.append(supervisor_assignment_log)

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

    # Transform log records and consolidate duplicates
    consolidated_logs = {}
    records_without_employee_id = []
    status_priority = {"Error": 3, "Exception": 2, "Success": 1}

    for log in log_records:
        record = {**log['properties'], "runid": log['ecid']}
        employee_id = record.get('employeeid')

        if not employee_id:
            records_without_employee_id.append(record)
            continue

        if employee_id not in consolidated_logs:
            consolidated_logs[employee_id] = record
        else:
            existing = consolidated_logs[employee_id]
            existing_priority = status_priority.get(existing.get('status'), 0)
            current_priority = status_priority.get(record.get('status'), 0)

            # Combine details
            combined_details = f"{existing.get('details', '')}; {record.get('details', '')}"
            combined_details = combined_details.strip('; ')

            # Keep record with higher priority, or existing if same priority
            if current_priority > existing_priority:
                consolidated_logs[employee_id] = {
                    **record, 'details': combined_details}
            else:
                consolidated_logs[employee_id] = {
                    **existing, 'details': combined_details}

    # Combine consolidated records with records that don't have employee_id
    final_log_records = list(consolidated_logs.values()
                             ) + records_without_employee_id

    rail.set_result(key="get_logged_success", val=len(
        list(filter(lambda item: item['status'] == "Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(
        list(filter(lambda item: item['status'] == "Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(
        list(filter(lambda item: item['status'] == "Exception", final_log_records))))

    return final_log_records

# Individual Group Analysis Functions for Conditional Creation


def get_parent_available_departments(full_path, existing_departments):
    """
    Analyze department hierarchy to find existing parent and child parts.
    Similar to PWC Global's get_parent_available function.

    Args:
        full_path: Full department path like 'iPipeline/Project Management/Client Services'
        existing_departments: Dict of existing department paths

    Returns:
        Dict with 'parents' (existing) and 'child' (to create) parts
    """
    path_parts = full_path.split('/')

    # Find how many levels already exist
    levels = 0
    for i in range(len(path_parts)):
        partial_path = '/'.join(path_parts[:i+1])
        if partial_path in existing_departments:
            levels = i + 1
        else:
            break

    # Return existing parent path and remaining child path to create
    if levels > 0:
        return {
            'parents': '/'.join(path_parts[:levels]),
            'child': '/'.join(path_parts[levels:]) if levels < len(path_parts) else ''
        }
    return {
        'parents': '',
        'child': full_path
    }


def analyze_departments_to_create(root_department):
    """
    Analyze user data to determine which department hierarchy chains need creation.
    Returns PWC-style hierarchy chains for parallel execution.

    Returns:
        List of hierarchy chain configs for rail.trigger_parallel_dagrun
        Example: [{'parents': 'iPipeline', 'child': 'Project Management/Client Services'}]
    """
    users_data = rail.load_all_records(rail.result("create_users_collection"))
    if not users_data:
        return []

    # Get existing departments
    existing_departments = rail.load_all_records(
        rail.result("get_all_department_groups_data"))
    department_lookup = {dept.get(
        'fullpath'): dept for dept in existing_departments if dept.get('fullpath')}

    # Extract unique full department hierarchies from user data
    full_department_paths = set()

    for user in users_data:
        dept_l1 = user.get('department_level_1', '').strip()
        dept_l2 = user.get('department_level_2', '').strip()

        if dept_l1:
            if dept_l2:
                # Full path with both levels
                full_department_paths.add(
                    f"{root_department}/{dept_l1}/{dept_l2}")
            else:
                # Only level 1
                full_department_paths.add(f"{root_department}/{dept_l1}")

    # For each full path, determine what needs to be created
    hierarchy_chains = []

    for full_path in full_department_paths:
        parent_analysis = get_parent_available_departments(
            full_path, department_lookup)

        # Only add if there's something to create
        if parent_analysis['child']:
            hierarchy_chains.append({
                'parents': parent_analysis['parents'],
                'child': parent_analysis['child']
            })

    return hierarchy_chains


def get_parent_available_locations(full_path, existing_locations):
    """
    Analyze location hierarchy to find existing parent and child parts.
    """
    path_parts = full_path.split('/')

    # Find how many levels already exist
    levels = 0
    for i in range(len(path_parts)):
        partial_path = '/'.join(path_parts[:i+1])
        if partial_path in existing_locations:
            levels = i + 1
        else:
            break

    # Return existing parent path and remaining child path to create
    if levels > 0:
        return {
            'parents': '/'.join(path_parts[:levels]),
            'child': '/'.join(path_parts[levels:]) if levels < len(path_parts) else ''
        }
    return {
        'parents': '',
        'child': full_path
    }


def analyze_locations_to_create():
    """
    Analyze user data to determine which location hierarchy chains need creation.
    Returns PWC-style hierarchy chains for parallel execution.

    Returns:
        List of hierarchy chain configs for rail.trigger_parallel_dagrun
    """
    users_data = rail.load_all_records(rail.result("create_users_collection"))
    if not users_data:
        return []

    # Get existing locations
    existing_locations = rail.load_all_records(
        rail.result("get_all_location_groups_data"))
    location_lookup = {
        loc.get('fullpath'): loc for loc in existing_locations if loc.get('fullpath')}

    # Extract unique full location hierarchies from user data
    full_location_paths = set()

    for user in users_data:
        location_l1 = user.get('location_level_1', '').strip()
        location_l2 = user.get('location_level_2', '').strip()

        if location_l1:
            if location_l2:
                # Full path with both levels
                full_location_paths.add(f"{location_l1}/{location_l2}")
            else:
                # Only level 1
                full_location_paths.add(location_l1)

    # For each full path, determine what needs to be created
    hierarchy_chains = []

    for full_path in full_location_paths:
        parent_analysis = get_parent_available_locations(
            full_path, location_lookup)

        # Only add if there's something to create
        if parent_analysis['child']:
            hierarchy_chains.append({
                'parents': parent_analysis['parents'],
                'child': parent_analysis['child']
            })

    return hierarchy_chains


def get_parent_available_employee_types(full_path, existing_employee_types):
    """
    Analyze employee type hierarchy to find existing parent and child parts.
    """
    path_parts = full_path.split('/')

    # Find how many levels already exist
    levels = 0
    for i in range(len(path_parts)):
        partial_path = '/'.join(path_parts[:i+1])
        if partial_path in existing_employee_types:
            levels = i + 1
        else:
            break

    # Return existing parent path and remaining child path to create
    if levels > 0:
        return {
            'parents': '/'.join(path_parts[:levels]),
            'child': '/'.join(path_parts[levels:]) if levels < len(path_parts) else ''
        }
    return {
        'parents': '',
        'child': full_path
    }


def analyze_employee_types_to_create():
    """
    Analyze user data to determine which employee type hierarchy chains need creation.
    Returns PWC-style hierarchy chains for parallel execution.

    Returns:
        List of hierarchy chain configs for rail.trigger_parallel_dagrun
    """
    users_data = rail.load_all_records(rail.result("create_users_collection"))
    if not users_data:
        return []

    # Get existing employee types
    existing_employee_types = rail.load_all_records(
        rail.result("get_all_employeetype_groups_data"))
    employee_type_lookup = {
        et.get('fullpath'): et for et in existing_employee_types if et.get('fullpath')}

    # Extract unique full employee type hierarchies from user data
    full_employee_type_paths = set()

    for user in users_data:
        category = user.get('employee_category', '').strip()
        schedule = user.get('employee_schedule', '').strip()
        emp_type = user.get('employee_type', '').strip()

        if category:
            if schedule:
                if emp_type:
                    # Full path with all 3 levels
                    full_employee_type_paths.add(
                        f"{category}/{schedule}/{emp_type}")
                else:
                    # Only category and schedule
                    full_employee_type_paths.add(f"{category}/{schedule}")
            else:
                # Only category
                full_employee_type_paths.add(category)

    # For each full path, determine what needs to be created
    hierarchy_chains = []

    for full_path in full_employee_type_paths:
        parent_analysis = get_parent_available_employee_types(
            full_path, employee_type_lookup)

        # Only add if there's something to create
        if parent_analysis['child']:
            hierarchy_chains.append({
                'parents': parent_analysis['parents'],
                'child': parent_analysis['child']
            })

    return hierarchy_chains


def check_if_departments_need_creation():
    """
    Check if department creation is needed.

    Returns:
        Boolean indicating if departments need to be created
    """
    departments_to_create = rail.result("analyze_departments_to_create", [])
    return len(departments_to_create) > 0


def check_if_locations_need_creation():
    """
    Check if location creation is needed.

    Returns:
        Boolean indicating if locations need to be created
    """
    locations_to_create = rail.result("analyze_locations_to_create", [])
    return len(locations_to_create) > 0


def check_if_employee_types_need_creation():
    """
    Check if employee type creation is needed.

    Returns:
        Boolean indicating if employee types need to be created
    """
    employee_types_to_create = rail.result(
        "analyze_employee_types_to_create", [])
    return len(employee_types_to_create) > 0


def analyze_project_roles_to_create():
    """
    Analyze user data to determine which project roles need creation.

    Returns:
        List of project role names (titles) that need to be created
    """
    users_data = rail.load_all_records(rail.result("create_users_collection"))
    if not users_data:
        return []

    # Get existing project roles
    existing_project_roles = rail.load_all_records(
        rail.result("get_all_project_roles"))
    existing_role_names = {role.get('name').lower(
    ) for role in existing_project_roles if role.get('name')}

    # Extract unique titles from user data
    unique_titles = set()

    for user in users_data:
        title = user.get('title', '').strip()
        if title:
            unique_titles.add(title)

    # Filter to only include titles that need creation as project roles
    project_roles_to_create = []
    for title in unique_titles:
        if title.lower() not in existing_role_names:
            project_roles_to_create.append({"project_role_name": title})

    return project_roles_to_create


def check_if_project_roles_need_creation():
    """
    Check if project role creation is needed.

    Returns:
        Boolean indicating if project roles need to be created
    """
    project_roles_to_create = rail.result(
        "analyze_project_roles_to_create", [])
    return len(project_roles_to_create) > 0


def get_currently_effective_item(schedule_data, current_date):
    """
    Find the currently effective item from a schedule array.

    Logic:
    - Only base item: return base item
    - Base + future items: return base item
    - Base + past/current items: return most recent past/current item
    """
    if not schedule_data:
        return null

    current_date = datetime.strptime(current_date, '%Y/%m/%d').date()

    base_item = null
    most_recent_item = null
    most_recent_date = null

    for item in schedule_data:
        effective_date = item.get('effectiveDate')

        if effective_date is null:
            base_item = item
            continue

        item_date = pendulum.date(
            effective_date['year'], effective_date['month'], effective_date['day'])

        if item_date <= current_date:
            if most_recent_date is null or item_date > most_recent_date:
                most_recent_item = item
                most_recent_date = item_date

    return most_recent_item if most_recent_item is not null else base_item


def get_email_log_details(log_file_path, dag_run, time_zone, STANDARD_EMAIL_DATE_FORMAT):
    current_time = pendulum.now(time_zone)
    start_time_str = dag_run.conf['process_start_time']
    return {
        "job_start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_file_name": dag_run.conf['log_filename'],
        "log_file_path": log_file_path,
        "total_record_count": dag_run.conf['total_record_count']
    }


@lru_cache(maxsize=128)
def get_all_user_login_names_from_feed(dag_run):
    """
    Extract all user login names from the user feed data.
    """
    users_login_names = rail.load_all_records(
        dag_run.conf.get("all_users_login_names"))
    if not users_login_names:
        return []

    login_names = [user.get('login_name').strip()
                   for user in users_login_names if user.get('login_name')]

    return login_names


def get_working_days(start_date, end_date):
    """
    Calculate working days between two dates (excluding Sat/Sun).
    O(1) time complexity using mathematical calculation.
    """
    # Convert to date objects
    if isinstance(start_date, dict):
        start = datetime(
            start_date['year'], start_date['month'], start_date['day']).date()
    elif isinstance(start_date, str):
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    elif isinstance(start_date, datetime):
        start = start_date.date()
    elif hasattr(start_date, 'year') and hasattr(start_date, 'month') and hasattr(start_date, 'day'):
        start = start_date if not hasattr(
            start_date, 'date') else start_date.date()
    else:
        raise TypeError(f"Unsupported type: {type(start_date)}")

    if isinstance(end_date, dict):
        end = datetime(end_date['year'],
                       end_date['month'], end_date['day']).date()
    elif isinstance(end_date, str):
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    elif isinstance(end_date, datetime):
        end = end_date.date()
    elif hasattr(end_date, 'year') and hasattr(end_date, 'month') and hasattr(end_date, 'day'):
        end = end_date if not hasattr(end_date, 'date') else end_date.date()
    else:
        raise TypeError(f"Unsupported type: {type(end_date)}")

    if start > end:
        start, end = end, start

    # Total days inclusive
    total_days = (end - start).days + 1

    # Full weeks contribute 5 working days each
    full_weeks, remaining_days = divmod(total_days, 7)
    working_days = full_weeks * 5

    # Count working days in the remaining partial week
    start_weekday = start.weekday()  # Mon=0, Sun=6
    for i in range(remaining_days):
        if (start_weekday + i) % 7 < 5:
            working_days += 1

    return working_days


def evaluate_seniority_condition(condition, seniority_years):
    """
    Evaluate a seniority condition against the employee's years of service.

    Supported operators:
    - ">=X": Greater than or equal to X years
    - "<=X": Less than or equal to X years
    - ">X": Greater than X years
    - "<X": Less than X years

    Args:
        condition: String like ">=0", ">=7", "<1", ">2", "<=2"
        seniority_years: Employee's years of service (int or float)

    Returns:
        Tuple of (is_match: bool, threshold: float)
        - is_match: Whether the condition is satisfied
        - threshold: The numeric threshold value extracted from condition
    """
    if not condition:
        return False, -1

    condition = condition.strip()

    if condition.startswith(">="):
        threshold = float(condition[2:])
        return seniority_years >= threshold, threshold
    elif condition.startswith("<="):
        threshold = float(condition[2:])
        return seniority_years <= threshold, threshold
    elif condition.startswith(">"):
        threshold = float(condition[1:])
        return seniority_years > threshold, threshold
    elif condition.startswith("<"):
        threshold = float(condition[1:])
        return seniority_years < threshold, threshold

    return False, -1


def get_matching_accrual_entry(timeoff_type_name, seniority_years, accrual_mapper):
    """
    Find the matching entry from the timeoff accrual mapper based on leave type and seniority.

    For leave types with multiple seniority tiers (like Canada_Vacation with <=2, >2, >=3, etc.),
    this function finds the MOST SPECIFIC matching tier for the employee's years of service.

    Matching Logic:
    1. Filter entries by leave_type matching timeoff_type_name
    2. For each matching entry, evaluate the seniority_condition
    3. Return the entry with the highest applicable tier (most specific match)

    Args:
        timeoff_type_name: Name of the timeoff type (e.g., "USA _Vacation", "Canada_Vacation")
        seniority_years: Employee's years of service
        accrual_mapper: List of accrual policy entries

    Returns:
        Dictionary with the matching accrual entry, or None if no match found
    """
    if not timeoff_type_name or not (seniority_years):
        return None

    seniority_years = float(seniority_years)

    # Find all entries matching the leave type
    matching_entries = [
        entry for entry in accrual_mapper
        if entry.get("leave_type") == timeoff_type_name
    ]

    if not matching_entries:
        return None

    # Evaluate seniority conditions and find the best match
    # For tiered policies, we want the highest applicable tier
    best_match = None
    best_threshold = -1

    for entry in matching_entries:
        condition = entry.get("seniority_condition", "")
        is_match, threshold = evaluate_seniority_condition(
            condition, seniority_years)

        if is_match and threshold > best_threshold:
            best_threshold = threshold
            best_match = entry

    return best_match


def get_accrual_details_from_config(dag_run, seniority_level, accrual_mapper):
    """
    Get the matching accrual entry based on dag_run configuration.

    Expected dag_run.conf keys:
    - timeoff_type_name: Name of the timeoff type
    - seniority_level: Employee's years of service

    Args:
        dag_run: Airflow DagRun object
        accrual_mapper: List of accrual policy entries

    Returns:
        Dictionary with the matching accrual entry details, or None if no match
    """
    timeoff_type_name = dag_run.conf.get("timeoff_type_name")

    if seniority_level is None:
        return None

    return get_matching_accrual_entry(timeoff_type_name, seniority_level, accrual_mapper)


def compare_date_dict(date1: dict, date2: dict) -> bool:
    """Compare two date dictionaries, return True if date1 > date2.

    Args:
        date1: Dict with 'day', 'month', 'year' keys
        date2: Dict with 'day', 'month', 'year' keys

    Returns:
        True if date1 is greater than date2, False otherwise
    """
    return (date1['year'], date1['month'], date1['day']) > (date2['year'], date2['month'], date2['day'])


def dict_date_to_datetime(dict_date):
    return datetime.strptime(str(dict_date['year']) + "/" + str(dict_date['month']) + "/" + str(dict_date['day']), "%Y/%m/%d").date()


def get_relevant_historical_policies(existing_timeoff_policysetschedule, effective_date_derived, current_date_format):
    if bool(existing_timeoff_policysetschedule and existing_timeoff_policysetschedule[0] and existing_timeoff_policysetschedule[0]['description']):
        count = 0
        for item in existing_timeoff_policysetschedule:
            if dict_date_to_datetime(item['effectiveDate']) < datetime.strptime(effective_date_derived, current_date_format).date():
                count += 1

        relevant_historical_policies = json.loads(json.dumps(existing_timeoff_policysetschedule[0:count]).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        return relevant_historical_policies

    return []


def modify_required_value_in_policy_set(policy_set_to_modify, script_description, key_uri, value_to_set):
    for x in policy_set_to_modify['timeOffBalanceEventScripts']:
        if x['script']['description'] == script_description:
            for y in x['additionalParameters']:
                if y['keyUri'] == key_uri:
                    y['value']['number'] = value_to_set
    return null


def get_modified_policyset_schedule(dag_run, REP_DATE_FORMAT, YMD_DATE_FORMAT):
    user_schedule = float(dag_run.conf.get('schedule_hours')
                          ) if dag_run.conf.get('schedule_hours') else 0
    user_fte = float(dag_run.conf.get('fte')) if dag_run.conf.get('fte') else 0
    user_start_date = datetime.strptime(
        dag_run.conf.get('user_start_date'), REP_DATE_FORMAT).date()
    user_seniority_level = float(dag_run.conf.get('seniority_level'))

    proration_effective_date_dict = dag_run.conf.get(
        'proration_effective_date')
    proration_effective_date = dict_date_to_datetime(
        proration_effective_date_dict)

    default_policysetschedule = dag_run.conf.get(
        'default_policyset_schedule_for_timeoff')
    default_policyset_for_modification = default_policysetschedule[0]['policySet']

    existing_policy_lines = rail.result(
        'get_relevant_historical_timeoff_policy_lines') if dag_run.conf.get("action") == "Update" else []

    if dag_run.conf.get('timeoff_reference_logic_type') == 'Type1-A1':
        # Put required monthly accrual amount for Type1-A1 logic
        if dag_run.conf.get('timeoff_type_name') == "USA _Vacation":
            calculated_yearly_entitlement = float(
                (20 * (user_schedule / 5)) + (user_seniority_level * (user_schedule / 5)))
        elif dag_run.conf.get('timeoff_type_name') == "Canada_Vacation":
            calculated_yearly_entitlement = float(
                (15 * (user_schedule / 5)) + (user_seniority_level * (user_schedule / 5)))

        yearly_accrual_cap = calculated_yearly_entitlement

        # Put required yearly entitlement for Type1-A1 logic
        modify_required_value_in_policy_set(
            default_policyset_for_modification, "Accrues time once per month.", "urn:replicon:script-key:parameter:accrual-annual-amount", calculated_yearly_entitlement)
        # Put required yearly accrual cap for Type1-A1 logic
        modify_required_value_in_policy_set(
            default_policyset_for_modification, "Limit how much time off the user can accrue in a year", "urn:replicon:script-key:parameter:maximum-accrual-amount", yearly_accrual_cap)

    else:
        matching_entry_from_accrual_mapper = rail.result(
            'get_matching_entry_from_accrual_mapper')
        yearly_accrual_rate = matching_entry_from_accrual_mapper.get(
            'yearly_accrual_rate')
        yearly_accrual_cap = matching_entry_from_accrual_mapper.get(
            'cap_accruals_for_year_hours')
        carry_forward_days = matching_entry_from_accrual_mapper.get(
            'carry_forward')

        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type1-A2':
            # Put required monthly accrual amount for Type1-A2 logic
            if yearly_accrual_rate:
                calculated_yearly_entitlement = float(
                    yearly_accrual_rate * (user_schedule/user_fte))
                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Accrues time once per month.", "urn:replicon:script-key:parameter:accrual-annual-amount", calculated_yearly_entitlement)

            # Put required yearly accrual cap for Type1-A2 logic
            if yearly_accrual_cap:
                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Limit how much time off the user can accrue in a year", "urn:replicon:script-key:parameter:maximum-accrual-amount", yearly_accrual_cap)

        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type1-B':
            # Put required monthly accrual amount for Type1-B logic
            if yearly_accrual_rate:
                calculated_yearly_entitlement = float(
                    yearly_accrual_rate * (user_schedule/5))
                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Accrues time once per month.", "urn:replicon:script-key:parameter:accrual-annual-amount", calculated_yearly_entitlement)

            # Put required yearly accrual cap for Type1-B logic
            if yearly_accrual_cap:
                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Limit how much time off the user can accrue in a year", "urn:replicon:script-key:parameter:maximum-accrual-amount", yearly_accrual_cap)

            # Carry Over Logic for Type1-B
            if carry_forward_days:
                # As per spec. -Assuming 5 days as schedule for user
                carry_forward_hours = float(
                    carry_forward_days * (user_schedule/5))
                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Carry over balance and expire if not used", "urn:replicon:script-key:parameter:carry-up-to-amount", carry_forward_hours)

        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type2-A':
            # Put required starting balance amount for Type2-A logic
            if yearly_accrual_rate:

                if dag_run.conf.get('action') == 'Update':
                    remaining_balance_for_timeoff = float(rail.result(
                        'get_current_balance_for_timeoff'))
                    previous_existing_schedule_for_user = float(dag_run.conf.get(
                        'previous_existing_schedule_for_user'))

                    updated_yearly_accrual_rate = (remaining_balance_for_timeoff/(previous_existing_schedule_for_user/5)) if (
                        remaining_balance_for_timeoff > 0) else 0
                    calculated_starting_balance = float(
                        updated_yearly_accrual_rate * (user_schedule/5))

                else:
                    calendar_year_end_date = rail.parse_date(
                        f"12/31/{user_start_date.year}", REP_DATE_FORMAT)
                    calendar_year_start_date = rail.parse_date(
                        f"01/01/{user_start_date.year}", REP_DATE_FORMAT)
                    proration_factor = float(get_working_days(user_start_date, calendar_year_end_date) / get_working_days(
                        calendar_year_start_date, calendar_year_end_date))

                    calculated_starting_balance = float(
                        (yearly_accrual_rate * proration_factor) * (user_schedule/5))

                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Set initial balance for the first day of a policy", "urn:replicon:script-key:parameter:amount", calculated_starting_balance)

        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type2-B':
            # Put required starting balance amount for Type2-B logic
            if yearly_accrual_rate:

                if dag_run.conf.get('action') == 'Update':
                    remaining_balance_for_timeoff = float(rail.result(
                        'get_current_balance_for_timeoff'))
                    previous_existing_schedule_for_user = float(dag_run.conf.get(
                        'previous_existing_schedule_for_user'))

                    updated_yearly_accrual_rate = (remaining_balance_for_timeoff/(previous_existing_schedule_for_user/5)) if (
                        remaining_balance_for_timeoff > 0) else 0
                    calculated_starting_balance = float(
                        updated_yearly_accrual_rate * (user_schedule/5))

                else:
                    calculated_starting_balance = float(
                        yearly_accrual_rate * (user_schedule/5))

                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Set initial balance for the first day of a policy", "urn:replicon:script-key:parameter:amount", calculated_starting_balance)

        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type3':
            # Put required monthly accrual amount for Type3 logic
            if yearly_accrual_rate:

                if dag_run.conf.get('action') == 'Update':
                    remaining_balance_for_timeoff = float(rail.result(
                        'get_current_balance_for_timeoff'))
                    previous_existing_schedule_for_user = float(dag_run.conf.get(
                        'previous_existing_schedule_for_user'))

                    updated_yearly_accrual_rate = (remaining_balance_for_timeoff/(previous_existing_schedule_for_user/5)) if (
                        remaining_balance_for_timeoff > 0) else 0
                    calculated_starting_balance = float(
                        updated_yearly_accrual_rate * (user_schedule/5))

                else:
                    # Create user_start_date dict with proration year for compare_date_dict
                    user_start_date_in_proration_year = {
                        'year': proration_effective_date.year, 'month': user_start_date.month, 'day': user_start_date.day}
                    if compare_date_dict(user_start_date_in_proration_year, proration_effective_date_dict):
                        service_year_start_date = rail.parse_date(
                            f"{user_start_date.month}/{user_start_date.day}/{str(int(proration_effective_date.year) - 1)}", REP_DATE_FORMAT)
                        service_year_end_date = rail.parse_date(
                            f"{user_start_date.month}/{user_start_date.day}/{proration_effective_date.year}", REP_DATE_FORMAT)
                    else:
                        service_year_start_date = rail.parse_date(
                            f"{user_start_date.month}/{user_start_date.day}/{proration_effective_date.year}", REP_DATE_FORMAT)
                        service_year_end_date = rail.parse_date(
                            f"{user_start_date.month}/{user_start_date.day}/{str(int(proration_effective_date.year) + 1)}", REP_DATE_FORMAT)
                    proration_factor = float(get_working_days(proration_effective_date, service_year_end_date) / get_working_days(
                        service_year_start_date, service_year_end_date))

                    calculated_starting_balance = float(
                        (yearly_accrual_rate * proration_factor) * (user_schedule/5))

                modify_required_value_in_policy_set(
                    default_policyset_for_modification, "Set initial balance for the first day of a policy", "urn:replicon:script-key:parameter:amount", calculated_starting_balance)

        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type4':
            # No modifications required for Type4 logic as per spec.
            pass
        if dag_run.conf.get('timeoff_reference_logic_type') == 'Type5':
            # No modifications required for Type5 logic as per spec.
            pass

    final_modified_policyset = json.loads(json.dumps(
        default_policyset_for_modification, ensure_ascii=False).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

    final_modified_policyset_schedule = [{
        "effectiveDate": rail.parse_date(dag_run.conf.get("current_date"), YMD_DATE_FORMAT) if dag_run.conf.get(
            "action") == "Update" else rail.get_replicon_date(user_start_date),
        "description": "Added by Integration",
        "policySet": final_modified_policyset
    }]

    # add existing historical policy lines if action is Update
    if dag_run.conf.get("action") == "Update":
        final_modified_policyset_schedule.extend(existing_policy_lines)

    return final_modified_policyset_schedule
