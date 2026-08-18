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
from dateutil.relativedelta import relativedelta
import json
import pendulum
import rail
from ipipeline.user_import.utils import request_payload

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
    locations_data = rail.load_all_records(rail.result("get_updated_location_groups_data"))
    departments_data = rail.load_all_records(rail.result("get_updated_department_groups_data"))
    employeetypes_data = rail.load_all_records(rail.result("get_updated_employeetype_groups_data"))
    org_roles_data = rail.load_all_records(rail.result("get_updated_servicecenter_groups_data"))
    projectroles_data = rail.load_all_records(rail.result("get_updated_project_roles"))
    activities_data = rail.result("get_required_activities")
    permissions_data = rail.result("get_required_permission_sets")

    result['calculated_orgrole_data'] = rail.result("get_updated_servicecenter_groups_data")
    
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
    result['calculated_timesheet_template'] = assignment_rule.get('timesheet_template') 
    result['calculated_work_week'] = assignment_rule.get('work_week')
    result['calculated_timesheet_and_time_entry_notification'] = assignment_rule.get('timesheet_and_time_entry_notification')
    # Convert activities list to dictionary with URIs
    activities_list = assignment_rule.get('activities', [])
    calculated_activities = {}
    if activities_list and activities_data:
        for activity_name in activities_list:
            if activity_name in activities_data:
                calculated_activities[activity_name] = activities_data[activity_name]
    result['calculated_activities'] = calculated_activities
    
    # Calculate time off types for assignment (only names and URIs)
    result['calculated_time_off_types'] = get_timeoff_types_for_assignment(user_data, config)
    result['calculated_payrule'] = assignment_rule.get('payrule')
    result['calculated_payrule_uri'] = rail.result("get_required_payrules").get(assignment_rule.get('payrule')) if assignment_rule.get('payrule') else null
    
    # Calculate timesheet period URI from assignment rule
    result['calculated_timesheet_period'] = assignment_rule.get('timesheet_period')
    result['calculated_timesheet_period_uri'] = rail.result("get_all_timesheet_periods").get(assignment_rule.get('timesheet_period')) if assignment_rule.get('timesheet_period') else null
    
    # Calculate schedule type URI based on assignment_rules_mapper schedule_type
    mapper_schedule_type = assignment_rule.get('schedule_type')  # "Office Schedule" or "Shift Schedule" from assignment_rules_mapper
    input_schedule_name = user_data.get('scheduled_hours')  # From SOURCE ABC file (40 hours, 37.5 hours, etc.)
    
    if mapper_schedule_type == 'Shift Schedule':
        # Shift schedule - hardcoded shift URI
        result['calculated_schedule_type_uri'] = 'urn:replicon:schedule-type:shift'
        result['calculated_office_schedule_uri'] = null
    elif mapper_schedule_type == 'Office Schedule':
        # Office schedule - hardcoded office schedule type URI + lookup office schedule name from get_all_office_schedules
        result['calculated_schedule_type_uri'] = 'urn:replicon:schedule-type:office-schedule'
        result['calculated_office_schedule_uri'] = rail.result("get_all_office_schedules").get(input_schedule_name) if input_schedule_name else null
    else:
        # No valid schedule type found
        result['calculated_schedule_type_uri'] = null
        result['calculated_office_schedule_uri'] = null
    
    result['calculated_schedule_name'] = input_schedule_name
    result['calculated_mapper_schedule_type'] = mapper_schedule_type
    
    # Extract approval paths and templates from assignment rule
    result['calculated_timesheet_approval_path'] = assignment_rule.get('timesheet_approval_path')
    result['calculated_time_entry_approval_path'] = assignment_rule.get('time_entry_approval_path')
    result['calculated_time_off_template'] = assignment_rule.get('time_off_template')
    result['calculated_time_off_approval'] = assignment_rule.get('time_off_approval')
    result['calculated_holiday_calendar'] = assignment_rule.get('holiday_calendar')
    
    # Step 9: Location URI lookup - Find appropriate location URI
    location_level_1 = user_data.get('location_level_1', null)
    location_level_2 = user_data.get('location_level_2', null)
    
    if location_level_2:
        # Use Level 2 location if available
        location_path = f"{location_level_1}/{location_level_2}"
        location_uri = find_group_uri_by_name_and_path(locations_data, location_level_2, location_path)
        calculated_location_name = location_level_2
    else:
        # Use Level 1 location
        location_path = location_level_1
        location_uri = find_group_uri_by_name_and_path(locations_data, location_level_1, location_level_1)
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
        department_uri = find_group_uri_by_name_and_path(departments_data, department_level_2, department_path)
        calculated_department_name = department_level_2
    else:
        # Use Level 1 department
        department_path = f"{defaults_mapper_data.get('root_department')}/{department_level_1}"
        department_uri = find_group_uri_by_name_and_path(departments_data, department_level_1, department_level_1)
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
        employee_type_uri = find_group_uri_by_name_and_path(employeetypes_data, employee_type, calculated_employee_type)
    elif employee_schedule:
        calculated_employee_type = f"{employee_category}/{employee_schedule}"
        employee_type_uri = find_group_uri_by_name_and_path(employeetypes_data, employee_schedule, calculated_employee_type)
    else:
        calculated_employee_type = employee_category
        employee_type_uri = find_group_uri_by_name_and_path(employeetypes_data, employee_category, calculated_employee_type)
    
    result['calculated_employee_type'] = calculated_employee_type
    result['calculated_employee_type_uri'] = employee_type_uri
    
    # Step 12: Rate card assignment - Find project role data by title match
    # Rate cards are derived directly from project roles, not from mapper
    project_role_data = find_project_role_data_by_title(
        user_data.get('title'),
        projectroles_data
    )
    result['calculated_project_role_uri'] = project_role_data.get('uri') if project_role_data else null
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
    user_permissions = get_permissions_by_orgrole_code(org_role_code, permissions_data, config.permissions_mapper_data, defaults_mapper_data)
    supervisor_uri = permissions_data.get(defaults_mapper_data.get("supervisor_permission")) if permissions_data else null
    result['calculated_supervisor_permission'] = supervisor_uri

    schedule_manager_supervisor_uri = permissions_data.get(defaults_mapper_data.get("schedule_manager_supervisor_permission")) if permissions_data else null
    result['calculated_schedule_manager_supervisor_permission'] = schedule_manager_supervisor_uri
    
    schedule_manager_not_supervisor_uri = permissions_data.get(defaults_mapper_data.get("schedule_manager_not_supervisor_permission")) if permissions_data else null
    result['calculated_schedule_manager_not_supervisor_permission'] = schedule_manager_not_supervisor_uri
    
    # PROCESS 3: Schedule Manager Permissions with location/department restrictions
    # Keep schedule manager permissions separate for child DAG to decide based on supervisor status
    schedule_manager_permissions = get_schedule_manager_permissions(user_data, locations_data,
        departments_data, schedule_manager_supervisor_uri, schedule_manager_not_supervisor_uri, defaults_mapper_data)
    
    result['calculated_permissions'] = user_permissions
    result['calculated_schedule_manager_permissions'] = schedule_manager_permissions
    result['calculated_orgrole_uri'] = org_role_uri
    result['calculated_orgrole_code'] = org_role_code
    
    # Step 13: Holiday calendar URI lookup
    holiday_calendar = result.get('calculated_holiday_calendar')
    result['calculated_holiday_calendar_uri'] = rail.result("get_required_holiday_calendars").get(holiday_calendar) if holiday_calendar else null
    
    # Step 14: Timesheet template URI lookup
    timesheet_template = result.get('calculated_timesheet_template')
    result['calculated_timesheet_template_uri'] = rail.result("get_required_timesheet_templates").get(timesheet_template) if timesheet_template else null
    
    return result

def get_assignment_rule_from_mapper(location_level_1, location_level_2, department_level_1, employee_category, assignment_rules_mapper_data):
    """
    Get complete assignment rule from comprehensive mapper based on tech spec requirements.
    
    Extracts: Schedule Type, Timesheet Template, Time Off Template, Holiday Calendar, Notifications
    Based on matching: Location L1, Location L2, Department L1, Employee Category
    
    Args:
        location_level_1: Country (US, UK, Canada, Japan)
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
        # Match Location Level 1 (exact match required)
        location1_match = (rule.get('location_level_1') == location_level_1)
        
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
                'time_off_approval': rule.get('time_off_approval')
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
        permission_uri = permissions_data.get(permission_name) if permissions_data else null
        calculated_permissions[permission_name] = permission_uri
    
    return calculated_permissions

def get_schedule_manager_permissions(user_data, locations_data, departments_data, schedule_manager_supervisor_uri, schedule_manager_not_supervisor_uri, defaults_mapper_data):
    """
    PROCESS 3: Schedule Manager Permissions with location/department restrictions.
    
    Restrictions based on user's location:
    - UK users: Can access Location Level 1 = UK
    - US users: Can access Location Level 1 = US AND Department Level 1 = Research & Development, Support
    """
    
    user_location_1 = user_data.get('location_level_1', None)
    
    restrictions = {
        "locations": [],
        "departments": []
    }
    
    # Apply restrictions based on user's location
    if user_location_1 == "United Kingdom":
        # UK users: Restricted to UK locations only
        if locations_data:
            for location in locations_data:
                if (location.get('name') in ["United Kingdom"] and 
                    location.get('length') == 1):
                    restrictions["locations"].append({
                        "name": location.get('name'),
                        "uri": location.get('uri')
                    })
                    break
    
    elif user_location_1 == "United States":
        # US users: Restricted to US locations AND specific departments
        if locations_data:
            for location in locations_data:
                if (location.get('name') in ["United States"] and 
                    location.get('length') == 1):
                    restrictions["locations"].append({
                        "name": location.get('name'),
                        "uri": location.get('uri')
                    })
                    break
        
        # Add department restrictions for US users
        if departments_data:
            restricted_departments = ["Research & Development", "Support"]
            for department in departments_data:
                if (department.get('name') in restricted_departments and 
                    department.get('length') == 2):
                    restrictions["departments"].append({
                        "name": department.get('name'),
                        "uri": department.get('uri')
                    })
    
    # Only include restrictions if they exist
    has_restrictions = bool(restrictions.get("locations") or restrictions.get("departments"))
    
    # Return both schedule manager permissions with the same restrictions
    result = {}
    
    if schedule_manager_supervisor_uri:
        result[defaults_mapper_data.get("schedule_manager_supervisor_permission")] = {
            "uri": schedule_manager_supervisor_uri,
            "restrictions": restrictions if has_restrictions else None
        }
    
    if schedule_manager_not_supervisor_uri:
        result[defaults_mapper_data.get("schedule_manager_not_supervisor_permission")] = {
            "uri": schedule_manager_not_supervisor_uri,
            "restrictions": restrictions if has_restrictions else None
        }
    
    return result

# Old complex time off function removed - now using simplified mapper from tech spec

def get_timeoff_types_for_assignment(user_data, config):
    """
    Calculate time off type assignments for master DAG.
    Policy schedules will be calculated later in get_user_creation_payload.
    
    Business Logic:
    1. Time off assignment based on Country, Location, and Level fields
    2. Visibility Control:
       - visible_to_employees=True: Auto-assigned through integration
       - visible_to_employees=False: Created but disabled, HR manages manually
    
    Args:
        user_data: User data with location, employee level
        config: Configuration with time_off_type_mapper_data
        
    Returns:
        Dictionary with calculated_time_off_types only
    """
    result = {}
    
    # Get timeoff types data from Replicon
    timeoff_types_data = rail.result("get_required_time_off_types")
    if not timeoff_types_data or not config.time_off_type_mapper_data:
        return {}
    
    # Extract user criteria for matching
    user_location_1 = user_data.get('location_level_1', null).upper()
    user_location_2 = user_data.get('location_level_2', null)
    user_employee_level = user_data.get('employee_level', null)
    
    # Results container
    calculated_time_off_types = {}  # Auto-assigned timeoff types (visible_to_employees=True)
    
    for policy in config.time_off_type_mapper_data:
        # Extract policy criteria
        location_level_1 = policy.get('location_level_1', null).upper()
        location_level_2_to_include = policy.get('location_level_2_to_include', [])
        location_level_2_to_exclude = policy.get('location_level_2_to_exclude', [])
        employee_levels_to_include = policy.get('employee_levels_to_include', [])
        employee_levels_to_exclude = policy.get('employee_levels_to_exclude', [])
        timeoff_type_name = policy.get('time_off_type', null)
        
        # Skip if timeoff type not available in Replicon
        if not timeoff_type_name or timeoff_type_name not in timeoff_types_data:
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
        
        # Policy matches user criteria - get timeoff URI and configuration
        timeoff_uri = timeoff_types_data[timeoff_type_name]
        
        calculated_time_off_types[timeoff_type_name] = {
            "uri": timeoff_uri,
            "visible_to_employees": policy.get("visible_to_employees"),
            "carry_forward": policy.get("carry_forward"),
            "carry_forward_expiry": policy.get("carry_forward_expiry"),
            "time_off_reset": policy.get("time_off_reset"),
            "over_draw": policy.get("over_draw"),
            "validation": policy.get("validation"),
            "have_accrual_conditions": bool(policy.get("accrual_conditions"))
        }
    
    return calculated_time_off_types

def get_timeoff_policy_accruals(user_data, config, start_date):
    """
    Get timeoff policy schedules with accrual calculations using calculated_time_off_types from user_data.
    Creates detailed policy schedules with FTE-based calculations for each assigned timeoff type.
    
    Args:
        user_data: User data containing calculated_time_off_types and FTE information
        config: Configuration with time_off_type_mapper_data
        start_date: User's employment start date in YYYY/MM/DD format
        
    Returns:
        Dictionary with timeoff names and their policy schedules with accrual details
    """
    # Extract user data
    calculated_time_off_types = user_data.get('calculated_time_off_types', {})
    if not calculated_time_off_types or not start_date:
        return {}
    
    # Extract user FTE and service information
    scheduled_hours = float(user_data.get('scheduled_hours'))
    fte_hours = float(user_data.get('fte'))
    current_service_years = get_service_years_from_start_date(start_date, user_data.get('current_date'), config.YMD_DATE_FORMAT)
    
    # Results container
    policy_schedules_by_timeoff = {}
    
    # Process each assigned timeoff type
    for timeoff_name, timeoff_config in calculated_time_off_types.items():
        timeoff_uri = timeoff_config.get("uri")
        # Find the policy configuration for this timeoff type
        policy_config = null
        # TODO: Optimize lookup by creating a dict from config.time_off_accrual_rates_mapper_data
        for policy in config.time_off_type_mapper_data:
            if policy.get('time_off_type') == timeoff_name:
                policy_config = policy
                break
        
        if not policy_config:
            continue
            
        accrual_conditions = policy_config.get('accrual_conditions')
        if not accrual_conditions or not accrual_conditions.get('tenure_rules'):
            # No accrual conditions - store basic info
            policy_schedules_by_timeoff[timeoff_name] = {
                'uri': timeoff_uri,
                'policy_schedules': [],
                'has_accrual': False
            }
            continue
        
        # Create policy schedules for this timeoff type
        policy_schedules = create_policy_schedules_for_timeoff_type(
            accrual_conditions, current_service_years, scheduled_hours, fte_hours, start_date, config.YMD_DATE_FORMAT
        )
        
        policy_schedules_by_timeoff[timeoff_name] = {
            'uri': timeoff_uri,
            'policy_schedules': policy_schedules,
            'has_accrual': True,
            'requires_policy_updates': accrual_conditions.get('policy_updates_required', False)
        }
    
    return policy_schedules_by_timeoff

def build_policy_schedule_from_default(dag_run, default_policy_response, timeoff_uri, fte_accrual_data, ymd_format):
    """
    Take default policy schedule as-is and update only FTE-related parameter values
    """
    if not default_policy_response or not fte_accrual_data:
        return []
    
    # Find FTE data for this timeoff type
    timeoff_fte_data = null
    for timeoff_type_name, timeoff_data in fte_accrual_data.items():
        if timeoff_data.get("uri") == timeoff_uri:
            timeoff_fte_data = timeoff_data
            break
    
    if not timeoff_fte_data or not timeoff_fte_data.get("policy_schedules"):
        # Transform default policy entries to required structure even without FTE calculations
        policy_schedule_entries = []
        for i, policy_entry in enumerate(default_policy_response):
            policy_set = policy_entry.get("policySet", {})
            start_offset = policy_entry.get("startOffset", {})
            offset_value = start_offset.get("offsetValue", 0)
            
            description = f"Effective {offset_value} years of service"
            # Use current date from DAG run as effective date for non-accrual policies
            current_date = datetime.strptime(dag_run.conf['current_date'], ymd_format)
            effective_date = {"year": current_date.year, "month": current_date.month, "day": current_date.day}
            
            policy_schedule_entries.append({
                'description': description,
                'effectiveDate': effective_date,
                'policySet': policy_set
            })
        
        return policy_schedule_entries
    
    # Transform default policy schedule entries to match required structure
    policy_schedule_entries = []
    fte_schedules = timeoff_fte_data["policy_schedules"]
    
    for i, policy_entry in enumerate(default_policy_response):
        if i < len(fte_schedules):
            fte_schedule = fte_schedules[i]
            policy_set = policy_entry.get("policySet", {})
            
            # Update values in timeOffBalanceEventScripts
            for script in policy_set.get("timeOffBalanceEventScripts", []):
                for param in script.get("additionalParameters", []):
                    key_uri = param.get("keyUri")
                    if key_uri == "urn:replicon:script-key:parameter:accrual-annual-amount":
                        param["value"]["number"] = fte_schedule.get("yearly_entitlement")
                    elif key_uri == "urn:replicon:script-key:parameter:accrual-monthly-amount":
                        param["value"]["number"] = fte_schedule.get("monthly_accrual")
                    elif key_uri == "urn:replicon:script-key:parameter:limitation-hours":
                        param["value"]["number"] = fte_schedule.get("limitation_hours")
            
            # Create policy schedule entry with required structure
            min_years = fte_schedule.get("min_years", 0)
            description = f"Effective {min_years} years of service"
            effective_date = fte_schedule.get("effectiveDate")
            
            policy_schedule_entries.append({
                'description': description,
                'effectiveDate': effective_date,
                'policySet': policy_set
            })
    
    return policy_schedule_entries

def build_timeoff_types_for_user_creation(dag_run, config):
    """
    Build timeoff types payload for user creation using default policies with FTE calculations
    """
    if not dag_run.conf.get("calculated_time_off_types"):
        return []
    
    default_policies_result = rail.result("get_default_timeoff_policies_for_create", [])
    
    # For user creation, use start date from input payload
    input_start_date = datetime.strptime(dag_run.conf.get('start_date'), config.REP_DATE_FORMAT).strftime(config.YMD_DATE_FORMAT) if dag_run.conf.get('start_date') else null
    fte_accrual_data = get_timeoff_policy_accruals(dag_run.conf, config, input_start_date)
    
    if not default_policies_result or not fte_accrual_data:
        # Fallback to simple timeoff assignment without policies
        timeoff_types = []
        for timeoff_config in dag_run.conf["calculated_time_off_types"].values():
            timeoff_uri = timeoff_config.get("uri")
            user_uri = rail.result("create_new_user").get("user", {}).get("uri")
            timeoff_types.append({
                "timeOffAccount": {
                    "userUri": user_uri,
                    "timeOffTypeUri": timeoff_uri
                },
                "policySetScheduleEntries": []
            })
        return timeoff_types
    
    # Build timeoff types with default policies and FTE calculations
    timeoff_types = []
    
    # For creates, use all calculated timeoff types
    timeoff_uris_to_process = [timeoff_config.get("uri") for timeoff_config in dag_run.conf["calculated_time_off_types"].values()]
    
    for timeoff_uri in timeoff_uris_to_process:
        # Find default policy for this timeoff type
        default_policy = null
        for policy_data in default_policies_result:
            if policy_data.get("timeOffUri") == timeoff_uri:
                default_policy = policy_data.get("defaultPolicy")
                break
        
        if default_policy:
            # Build policy schedule with FTE calculations
            policy_schedule = build_policy_schedule_from_default(dag_run, default_policy, timeoff_uri, fte_accrual_data, config.YMD_DATE_FORMAT)
        else:
            # Fallback to empty policy schedule
            policy_schedule = []
        
        user_uri = rail.result("create_new_user").get("user", {}).get("uri")
        timeoff_types.append({
            "timeOffAccount": {
                "userUri": user_uri,
                "timeOffTypeUri": timeoff_uri
            },
            "policySetScheduleEntries": policy_schedule
        })
    
    return timeoff_types

def build_comprehensive_timeoff_assignments_for_update(dag_run, config):
    """
    Build comprehensive timeoff assignments for user updates handling all scenarios:
    1. Location/Level changes only -> Add/remove timeoff types with default policies
    2. FTE/Schedule changes only -> Add new policy lines to existing overlapping types (only for accrual types)
    3. Both changes -> Handle new types + add policy lines to overlapping types
    
    Uses have_accrual_conditions flag from payload to determine processing:
    - Types with accrual conditions: Full FTE processing + new policy lines on FTE changes
    - Types without accrual conditions: Default policies only, no FTE processing
    
    Key Logic:
    - New policy lines ONLY created when FTE/Schedule changes AND type has accrual conditions
    - Location/Level changes only add/remove timeoff types with default policies
    """
    all_timeoff_types = dag_run.conf.get("calculated_time_off_types", {})
    if not all_timeoff_types:
        return []
    
    # Get all required data
    user_details_result = rail.result("get_user_details", {})
    default_policies_result = rail.result("get_default_timeoff_policies", [])
    timeoff_changes = request_payload.get_updated_timeoff_types(dag_run)
    change_details = request_payload.check_fte_or_schedule_or_location_or_level_changes(dag_run)
    
    if not user_details_result or not default_policies_result:
        return []
    
    # Get user URI from update result
    user_uri = rail.result("update_user_details", {}).get("user", {}).get("uri")
    if not user_uri:
        return []
    
    # Extract change types
    fte_changed = change_details.get('is_fte_changed', False)
    schedule_changed = change_details.get('is_scheduled_hours_changed', False)
    location_changed = change_details.get('is_location_changed', False)
    level_changed = change_details.get('is_level_changed', False)
    
    # Categorize change scenarios
    fte_or_schedule_changed = fte_changed or schedule_changed
    location_or_level_changed = location_changed or level_changed
    
    # Extract current timeoff policies from user details
    current_policies = {}
    timeoff_policy_summary = user_details_result.get("timeOffTypePolicySummary", {})
    current_timeoff_assignments = timeoff_policy_summary.get("policiesByTimeOffType", [])
    
    for policy in current_timeoff_assignments:
        timeoff_type = policy.get("timeOffType", {})
        type_name = timeoff_type.get("displayText")
        type_uri = timeoff_type.get("uri")
        is_allowed = policy.get("isTimeOffAllowedAgainstThisTimeOffType")
        
        if type_name and type_uri and is_allowed:
            # Transform existing policies (replace 'script' with 'scriptTarget')
            existing_schedule = policy.get("policySetSchedule", [])
            transformed_schedule = json.loads(json.dumps(
                existing_schedule, ensure_ascii=False).replace('"script"', '"scriptTarget"'))
            
            current_policies[type_name] = {
                "uri": type_uri,
                "existing_policy_schedule": transformed_schedule
            }
    
    # Get proper start date for service years calculation
    employment_date_range = user_details_result.get("userDetails", {}).get("employmentDateRange", {})
    replicon_start_date = employment_date_range.get("startDate") if employment_date_range else null
    if replicon_start_date:
        start_date = f'{replicon_start_date["year"]}/{replicon_start_date["month"]:02d}/{replicon_start_date["day"]:02d}'
    else:
        # Fallback to dag_run start_date if available
        start_date = datetime.strptime(dag_run.conf.get('start_date'), config.REP_DATE_FORMAT).strftime(config.YMD_DATE_FORMAT) if dag_run.conf.get('start_date') else null
    
    # Calculate FTE accruals if FTE or scheduled hours changed
    fte_accrual_data = {}
    if fte_or_schedule_changed:
        fte_accrual_data = get_timeoff_policy_accruals(dag_run.conf, config, start_date)
    
    # Build assignments based on change scenarios
    timeoff_assignments = []
    
    # SCENARIO 1: Location/Level changes only (NO FTE/Schedule changes)
    if location_or_level_changed and not fte_or_schedule_changed:
        
        # Handle NEW timeoff types with DEFAULT policies
        new_timeoff_types = timeoff_changes.get("new_timeoff_types", [])
        
        for timeoff_uri in new_timeoff_types:
            # Find timeoff config to check accrual conditions
            timeoff_config = find_timeoff_config_by_uri(timeoff_uri, all_timeoff_types)
            
            # Find default policy for this timeoff type
            default_policy = null
            for policy_data in default_policies_result:
                if policy_data.get("timeOffUri") == timeoff_uri:
                    default_policy = policy_data.get("defaultPolicy")
                    break
            
            if default_policy:
                if timeoff_config and timeoff_config.get("have_accrual_conditions"):
                    # Build policy schedule with current accruals (not FTE updated ones)
                    current_accrual_data = get_timeoff_policy_accruals(dag_run.conf, config, start_date)
                    policy_schedule = build_policy_schedule_from_default(dag_run, default_policy, timeoff_uri, current_accrual_data, config.YMD_DATE_FORMAT)
                else:
                    # Simple default policy for non-accrual types
                    policy_schedule = build_simple_default_policy_schedule(dag_run, default_policy, config.YMD_DATE_FORMAT)
            else:
                # Fallback to empty policy schedule
                policy_schedule = []
            
            timeoff_assignments.append({
                "timeOffAccount": {
                    "userUri": user_uri,
                    "timeOffTypeUri": timeoff_uri
                },
                "policySetScheduleEntries": policy_schedule
            })
        
        # Handle OVERLAPPING timeoff types - keep existing policies unchanged
        overlapping_timeoff_types = timeoff_changes.get("overlapping_timeoff_types", [])
        for timeoff_uri in overlapping_timeoff_types:
            # Find the timeoff type name
            timeoff_type_name = find_timeoff_name_by_uri(timeoff_uri, all_timeoff_types)
            
            if timeoff_type_name and timeoff_type_name in current_policies:
                existing_policies = current_policies[timeoff_type_name]["existing_policy_schedule"]
                
                timeoff_assignments.append({
                    "timeOffAccount": {
                        "userUri": user_uri,
                        "timeOffTypeUri": timeoff_uri
                    },
                    "policySetScheduleEntries": existing_policies
                })
    
    # SCENARIO 2: FTE/Schedule changes only (NO Location/Level changes)
    elif fte_or_schedule_changed and not location_or_level_changed:
        
        # Only handle OVERLAPPING timeoff types - add new policy lines only for accrual types
        overlapping_timeoff_types = timeoff_changes.get("overlapping_timeoff_types", [])
        for timeoff_uri in overlapping_timeoff_types:
            # Find timeoff config and type name
            timeoff_config = find_timeoff_config_by_uri(timeoff_uri, all_timeoff_types)
            timeoff_type_name = find_timeoff_name_by_uri(timeoff_uri, all_timeoff_types)
            
            if timeoff_type_name and timeoff_type_name in current_policies:
                existing_policies = current_policies[timeoff_type_name]["existing_policy_schedule"]
                
                # Only add new policy lines for types with accrual conditions
                if timeoff_config and timeoff_config.get("have_accrual_conditions"):
                    # Find default policy for new calculations
                    default_policy = null
                    for policy_data in default_policies_result:
                        if policy_data.get("timeOffUri") == timeoff_uri:
                            default_policy = policy_data.get("defaultPolicy")
                            break
                    
                    if default_policy and len(default_policy) > 0:
                        combined_policies = handle_policy_line_overlap(
                            existing_policies, default_policy[0], timeoff_uri, 
                            fte_accrual_data, start_date, dag_run, config
                        )
                    else:
                        # Keep existing policies only if no default policy found
                        combined_policies = existing_policies
                else:
                    # Non-accrual types: keep existing policies unchanged
                    combined_policies = existing_policies
                
                timeoff_assignments.append({
                    "timeOffAccount": {
                        "userUri": user_uri,
                        "timeOffTypeUri": timeoff_uri
                    },
                    "policySetScheduleEntries": combined_policies
                })
    
    # SCENARIO 3: Both Location/Level AND FTE/Schedule changes
    elif location_or_level_changed and fte_or_schedule_changed:
        
        # Handle NEW timeoff types with DEFAULT policies (based on updated FTE if accrual type)
        new_timeoff_types = timeoff_changes.get("new_timeoff_types", [])
        
        for timeoff_uri in new_timeoff_types:
            # Find timeoff config to check accrual conditions
            timeoff_config = find_timeoff_config_by_uri(timeoff_uri, all_timeoff_types)
            
            # Find default policy for this timeoff type
            default_policy = null
            for policy_data in default_policies_result:
                if policy_data.get("timeOffUri") == timeoff_uri:
                    default_policy = policy_data.get("defaultPolicy")
                    break
            
            if default_policy:
                if timeoff_config and timeoff_config.get("have_accrual_conditions"):
                    # Build policy schedule with updated FTE calculations
                    policy_schedule = build_policy_schedule_from_default(dag_run, default_policy, timeoff_uri, fte_accrual_data, config.YMD_DATE_FORMAT)
                else:
                    # Simple default policy for non-accrual types
                    policy_schedule = build_simple_default_policy_schedule(dag_run, default_policy, config.YMD_DATE_FORMAT)
            else:
                # Fallback to empty policy schedule
                policy_schedule = []
            
            timeoff_assignments.append({
                "timeOffAccount": {
                    "userUri": user_uri,
                    "timeOffTypeUri": timeoff_uri
                },
                "policySetScheduleEntries": policy_schedule
            })
        
        # Handle OVERLAPPING timeoff types - add new policy lines with updated FTE (only for accrual types)
        overlapping_timeoff_types = timeoff_changes.get("overlapping_timeoff_types", [])
        for timeoff_uri in overlapping_timeoff_types:
            # Find timeoff config and type name
            timeoff_config = find_timeoff_config_by_uri(timeoff_uri, all_timeoff_types)
            timeoff_type_name = find_timeoff_name_by_uri(timeoff_uri, all_timeoff_types)
            
            if timeoff_type_name and timeoff_type_name in current_policies:
                existing_policies = current_policies[timeoff_type_name]["existing_policy_schedule"]
                
                # Only add new policy lines for types with accrual conditions
                if timeoff_config and timeoff_config.get("have_accrual_conditions"):
                    # Find default policy for new calculations
                    default_policy = null
                    for policy_data in default_policies_result:
                        if policy_data.get("timeOffUri") == timeoff_uri:
                            default_policy = policy_data.get("defaultPolicy")
                            break
                    
                    if default_policy and len(default_policy) > 0:
                        combined_policies = handle_policy_line_overlap(
                            existing_policies, default_policy[0], timeoff_uri,
                            fte_accrual_data, start_date, dag_run, config
                        )
                    else:
                        # Keep existing policies only if no default policy found
                        combined_policies = existing_policies
                else:
                    # Non-accrual types: keep existing policies unchanged
                    combined_policies = existing_policies
                
                timeoff_assignments.append({
                    "timeOffAccount": {
                        "userUri": user_uri,
                        "timeOffTypeUri": timeoff_uri
                    },
                    "policySetScheduleEntries": combined_policies
                })
    
    # SCENARIO 4: No relevant changes - return empty
    else:
        return []
    
    return timeoff_assignments

def find_timeoff_config_by_uri(target_uri, all_timeoff_types):
    """Find timeoff configuration by URI"""
    for type_name, config_data in all_timeoff_types.items():
        if config_data.get("uri") == target_uri:
            return config_data
    return null

def find_timeoff_name_by_uri(target_uri, all_timeoff_types):
    """Find timeoff type name by URI"""
    for type_name, config_data in all_timeoff_types.items():
        if config_data.get("uri") == target_uri:
            return type_name
    return null

def build_simple_default_policy_schedule(dag_run, default_policy_response, ymd_format):
    """
    Build simple policy schedule for non-accrual timeoff types
    Uses default policy as-is without FTE calculations
    """
    if not default_policy_response:
        return []
    
    policy_schedule_entries = []
    current_date = datetime.strptime(dag_run.conf['current_date'], ymd_format)
    
    for i, policy_entry in enumerate(default_policy_response):
        policy_set = policy_entry.get("policySet", {})
        start_offset = policy_entry.get("startOffset", {})
        offset_value = start_offset.get("offsetValue", 0)
        
        description = f"Effective {offset_value} years of service"
        effective_date = {"year": current_date.year, "month": current_date.month, "day": current_date.day}
        
        policy_schedule_entries.append({
            'description': description,
            'effectiveDate': effective_date,
            'policySet': policy_set
        })
    
    return policy_schedule_entries

def handle_policy_line_overlap(existing_policies, default_policy_template, timeoff_uri, fte_accrual_data, start_date, dag_run, config):
    """
    Handle policy line overlap by updating existing entries with same effective date
    or adding new policy line if no overlap found
    """
    current_date = rail.parse_date(dag_run.conf["current_date"], config.YMD_DATE_FORMAT)
    
    # Check if there's already a policy line with the same effective date
    existing_policy_found = False
    updated_policies = []
    
    for existing_entry in existing_policies:
        existing_date = existing_entry.get("effectiveDate", {})
        
        # Check if dates match (year, month, day)
        if (existing_date.get("year") == current_date["year"] and 
            existing_date.get("month") == current_date["month"] and 
            existing_date.get("day") == current_date["day"]):
            
            # Update existing policy line with new FTE values
            updated_entry = json.loads(json.dumps(existing_entry))  # Deep copy
            updated_entry = update_policy_with_fte_values(updated_entry, timeoff_uri, fte_accrual_data, start_date, dag_run.conf.get('current_date', ''), config)
            updated_entry["description"] = f"Updated {dag_run.conf['current_date']}"
            updated_policies.append(updated_entry)
            existing_policy_found = True
        else:
            # Keep existing policy as-is
            updated_policies.append(existing_entry)
    
    # If no existing policy found with same date, add new policy line
    if not existing_policy_found:
        # Create new policy entry from default policy template
        new_policy_entry = json.loads(json.dumps(default_policy_template))  # Deep copy
        
        # Update effective date to current date
        new_policy_entry["effectiveDate"] = current_date
        new_policy_entry["description"] = f"Effective {dag_run.conf['current_date']}"
        
        # Update with FTE values
        new_policy_entry = update_policy_with_fte_values(new_policy_entry, timeoff_uri, fte_accrual_data, start_date, dag_run.conf.get('current_date', ''), config)
        
        # Add new policy line to existing policies
        updated_policies.append(new_policy_entry)
    
    return updated_policies

def update_policy_with_fte_values(policy_entry, timeoff_uri, fte_accrual_data, start_date, current_date, config):
    """
    Helper function to update policy entry with FTE-calculated values
    """
    if not fte_accrual_data or not start_date:
        return policy_entry
    
    # Calculate current service years
    current_service_years = get_service_years_from_start_date(
        start_date, 
        current_date, 
        config.YMD_DATE_FORMAT
    )

    # Find the appropriate policy values based on current service years
    for timeoff_name, timeoff_data in fte_accrual_data.items():
        if timeoff_data.get('uri') == timeoff_uri:
            policy_schedules = timeoff_data.get('policy_schedules', [])
            # Find the policy that applies to current service years
            for policy in policy_schedules:
                min_years = policy.get('min_years', 0)
                max_years = policy.get('max_years', 999)
                
                if min_years <= current_service_years < max_years:
                    # Update FTE values in the policy structure
                    policy_set = policy_entry.get("policySet", {})
                    
                    # Update values in timeOffBalanceEventScripts
                    for script in policy_set.get("timeOffBalanceEventScripts", []):
                        for param in script.get("additionalParameters", []):
                            key_uri = param.get("keyUri")
                            if key_uri == "urn:replicon:script-key:parameter:accrual-annual-amount":
                                param["value"]["number"] = policy.get("yearly_entitlement", 0)
                            elif key_uri == "urn:replicon:script-key:parameter:accrual-monthly-amount":
                                param["value"]["number"] = policy.get("monthly_accrual", 0)
                            elif key_uri == "urn:replicon:script-key:parameter:limitation-hours":
                                param["value"]["number"] = policy.get("limitation_hours", 0)
                    break
            break
    
    return policy_entry

def create_policy_schedules_for_timeoff_type(accrual_conditions, current_service_years, scheduled_hours, fte_hours, start_date, ymd_date_format):
    """
    Create policy schedules for a specific timeoff type based on accrual conditions.
    """
    tenure_rules = accrual_conditions.get('tenure_rules', [])
    fte_prorated = accrual_conditions.get('fte_prorated', True)
    
    # Calculate FTE ratio for proration
    fte_ratio = 1.0
    if fte_prorated and fte_hours > 0:
        fte_ratio = scheduled_hours / fte_hours
    
    policy_schedules = []
    
    for rule in tenure_rules:
        min_years = rule.get('min_years', 0)
        max_years = rule.get('max_years', 999)
        base_monthly_rate = rule.get('rate', 0)
        base_limitation = rule.get('limitation_hours', 0)
        
        # Apply FTE proration to accrual amounts
        prorated_monthly_accrual = base_monthly_rate * fte_ratio
        prorated_yearly_accrual = prorated_monthly_accrual * 12  # Convert to yearly entitlement
        prorated_limitation = base_limitation * fte_ratio if base_limitation > 0 else 0
        
        # Calculate effective date for this tier
        # If user hasn't reached this tier yet, calculate future anniversary date
        # If user has already reached this tier, use their start date or calculated anniversary
        effective_date = calculate_tier_effective_date(start_date, min_years, ymd_date_format)
        
        # Create tier description
        if max_years >= 999:
            tier_description = f"After {min_years} Years of Service"
        else:
            tier_description = f"Years {min_years}-{max_years-1} of Service"
        
        policy_schedule = {
            "effectiveDate": effective_date,
            "tier_description": tier_description,
            "min_years": min_years,
            "max_years": max_years,
            "yearly_entitlement": round(prorated_yearly_accrual, 2),
            "monthly_accrual": round(prorated_monthly_accrual, 2),
            "limitation_hours": round(prorated_limitation, 2),
            "base_monthly_rate": base_monthly_rate,
            "fte_ratio": round(fte_ratio, 2),
            "fte_prorated": fte_prorated
        }
        
        policy_schedules.append(policy_schedule)
    
    # Sort by min_years to ensure proper order
    policy_schedules.sort(key=lambda x: x['min_years'])
    
    return policy_schedules

def calculate_tier_effective_date(start_date, min_years, ymd_date_format):
    """
    Calculate effective date for a service year tier.
    
    Args:
        start_date: User's start date string
        min_years: Minimum years for this tier
        current_service_years: User's current service years
        rep_date_format: Date format for start_date
        
    Returns:
        Effective date for this tier (dict with year, month, day)
    """
    # Parse start date
    start_dt = datetime.strptime(start_date, ymd_date_format)
    
    # Calculate the anniversary date for this tier
    tier_anniversary = start_dt + relativedelta(years=int(min_years))
    
    return {
        "year": tier_anniversary.year,
        "month": tier_anniversary.month, 
        "day": tier_anniversary.day
    }


def get_service_years_from_start_date(start_date_str, current_date_str, ymd_format):
    """
    Calculate service years from start date string.
    
    Args:
        start_date_str: Start date string
        current_date_str: Current date from master DAG
        ymd_format: YMD date format from config
        rep_date_format: Date format for start_date
        
    Returns:
        Number of service years (int)
    """
    if not start_date_str or not current_date_str:
        return 0
    
    # Parse start date
    start_date = datetime.strptime(start_date_str, ymd_format)
    
    # Parse current date using config format
    current_date = datetime.strptime(current_date_str, ymd_format)
    
    # Calculate years of service using relativedelta
    delta = relativedelta(current_date, start_date)
    
    # Return whole years of service
    return max(0, delta.years)

def logging_details(time_zone, STANDARD_EMAIL_DATE_FORMAT, YMD_DATE_FORMAT):
    """
    Log import start details
    """
    today = pendulum.now(time_zone)
    file_name = rail.render_template("{{ result('new_file_sensor') | file_base }}")
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
    logs = (rail.result("gather_user_logs") if rail.result("gather_user_logs") else []) + [rail.result("create_groups_log")]
    
    # Add supervisor assignment logs if they exist
    supervisor_assignment_log = rail.result("create_supervisor_assignment_log") if rail.result("create_supervisor_assignment_log") else null
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
                consolidated_logs[employee_id] = {**record, 'details': combined_details}
            else:
                consolidated_logs[employee_id] = {**existing, 'details': combined_details}
    
    # Combine consolidated records with records that don't have employee_id
    final_log_records = list(consolidated_logs.values()) + records_without_employee_id

    rail.set_result(key="get_logged_success", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))

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
    existing_departments = rail.load_all_records(rail.result("get_all_department_groups_data"))
    department_lookup = {dept.get('fullpath'): dept for dept in existing_departments if dept.get('fullpath')}
    
    # Extract unique full department hierarchies from user data
    full_department_paths = set()
    
    for user in users_data:
        dept_l1 = user.get('department_level_1', '').strip()
        dept_l2 = user.get('department_level_2', '').strip()
        
        if dept_l1:
            if dept_l2:
                # Full path with both levels
                full_department_paths.add(f"{root_department}/{dept_l1}/{dept_l2}")
            else:
                # Only level 1
                full_department_paths.add(f"{root_department}/{dept_l1}")
    
    # For each full path, determine what needs to be created
    hierarchy_chains = []
    
    for full_path in full_department_paths:
        parent_analysis = get_parent_available_departments(full_path, department_lookup)
        
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
    existing_locations = rail.load_all_records(rail.result("get_all_location_groups_data"))
    location_lookup = {loc.get('fullpath'): loc for loc in existing_locations if loc.get('fullpath')}
    
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
        parent_analysis = get_parent_available_locations(full_path, location_lookup)
        
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
    existing_employee_types = rail.load_all_records(rail.result("get_all_employeetype_groups_data"))
    employee_type_lookup = {et.get('fullpath'): et for et in existing_employee_types if et.get('fullpath')}
    
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
                    full_employee_type_paths.add(f"{category}/{schedule}/{emp_type}")
                else:
                    # Only category and schedule
                    full_employee_type_paths.add(f"{category}/{schedule}")
            else:
                # Only category
                full_employee_type_paths.add(category)
    
    # For each full path, determine what needs to be created
    hierarchy_chains = []
    
    for full_path in full_employee_type_paths:
        parent_analysis = get_parent_available_employee_types(full_path, employee_type_lookup)
        
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
    employee_types_to_create = rail.result("analyze_employee_types_to_create", [])
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
    existing_project_roles = rail.load_all_records(rail.result("get_all_project_roles"))
    existing_role_names = {role.get('name') for role in existing_project_roles if role.get('name')}
    
    # Extract unique titles from user data
    unique_titles = set()
    
    for user in users_data:
        title = user.get('title', '').strip()
        if title:
            unique_titles.add(title)
    
    # Filter to only include titles that need creation as project roles
    project_roles_to_create = []
    for title in unique_titles:
        if title not in existing_role_names:
            project_roles_to_create.append({"project_role_name": title})
    
    return project_roles_to_create

def check_if_project_roles_need_creation():
    """
    Check if project role creation is needed.
    
    Returns:
        Boolean indicating if project roles need to be created
    """
    project_roles_to_create = rail.result("analyze_project_roles_to_create", [])
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
        
        item_date = pendulum.date(effective_date['year'], effective_date['month'], effective_date['day'])
        
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
    users_login_names = rail.load_all_records(dag_run.conf.get("all_users_login_names"))
    if not users_login_names:
        return []
    
    login_names = [user.get('login_name').strip() for user in users_login_names if user.get('login_name')]

    return login_names
