import uuid
import json
from datetime import datetime
import rail
from tsystems.project_import_v4.config import STATUS_MAPPING, EXTERNAL_EMPLOYEE_TYPES
from tsystems.project_import_v4.utils import request_payload

_MOJIBAKE_CANDIDATE_ENCODINGS = ('mac_roman', 'cp1252', 'latin-1')

def fix_text_encoding(value):
    if not isinstance(value, str) or not value:
        return value
    for encoding in _MOJIBAKE_CANDIDATE_ENCODINGS:
        try:
            candidate = value.encode(encoding).decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if candidate != value:
            return candidate
    return value

def get_new_team_member_uris(existing_team_members, proposed_uris):
    existing_uris = {
        m.get('resource', {}).get('uri')
        for m in (existing_team_members or [])
        if isinstance(m, dict) and isinstance(m.get('resource'), dict)
    }
    existing_uris.discard(None)
    return [uri for uri in (proposed_uris or []) if uri not in existing_uris]

_PROJECT_MANAGEMENT_POLICY_URI = 'urn:replicon:policy:project-management'

def get_missing_pm_permission_sets(assigned_permissions, pm_permission_set_uri):
    if not pm_permission_set_uri:
        return []
    for perm in (assigned_permissions or []):
        if isinstance(perm, dict) and perm.get('policyUri') == _PROJECT_MANAGEMENT_POLICY_URI:
            return []
    return [pm_permission_set_uri]

def get_missing_mandatory_fields_message(item):
    """Generate detailed error message listing all missing mandatory fields"""
    mandatory_fields = {
        'project_code': 'Code',
        'project_name': 'Name',
        'start_date': 'Start Date',
        'status': 'Status',
        'cost_center': 'Cost Center',
        'accounting_area': 'Org Structure'
    }

    missing_fields = []
    for field_key, field_name in mandatory_fields.items():
        if not item.get(field_key):
            missing_fields.append(field_name)

    if missing_fields:
        return f"{', '.join(missing_fields)} not present in the input payload"
    return "Mandatory field is missing"

def get_payload_data(dag_run):
    """
    Extract project payload data from DAG run configuration
    Processes the nested webhook structure from TARDIS middleware
    NOTE: This function is NOT used in the current API-based integration
    Padding is applied in normalize_project_data() for QA testing
    """
    # Navigate the nested webhook payload structure: conf > webhook > data > costobject
    conf_data = dag_run.conf.get('payload', {}).get('data', {}).get('costobject', {})

    # Extract values (padding applied in normalize_project_data)
    cost_center = extract_related_unit(conf_data, "Profit Center")
    accounting_area = extract_related_unit(conf_data, "Accounting Area")
    delivery_cost_center = extract_related_unit(conf_data, "Cost Center")

    return {
        # Core project identification
        'project_id': conf_data.get("id", ""),
        'project_name': conf_data.get("name", ""),
        'description': conf_data.get("description", ""),
        
        # Project timeline
        'start_date': conf_data.get("validFor", {}).get("startDateTime", ""),
        'end_date': conf_data.get("validFor", {}).get("endDateTime", ""),

        # Status and organizational mapping
        'status': conf_data.get("status", {}).get("sourceKey", ""),
        'cost_center': cost_center,
        'accounting_area': accounting_area,
        # Client and project manager relationships
        'client_code': extract_related_party_id(conf_data, "End Customer"),
        'project_manager_id': extract_related_party_id(conf_data, "Project Manager"),
        # Billing and financial configuration
        'billing_type': conf_data.get("billingDetail", {}).get("billingType", ""),
        'cost_type': conf_data.get("billingDetail", {}).get("costType", ""),
        'time_expense_entry': conf_data.get("billingDetail", {}).get("timeExpenceEntry", ""),
        'accounting_group': conf_data.get("billingDetail", {}).get("settlementGroup", ""),
        # Project categorization and control
        'project_type': conf_data.get("category", {}).get("sourceKey", ""),
        'control_expert': conf_data.get("category", {}).get("sourceControl", ""),
        'process_id_group': dag_run.conf.get('payload', {}).get('source', ""),
        'delivery_cost_center': delivery_cost_center,
        'contract_type': conf_data.get("contractType", "")
    }

# Define mandatory fields required for project processing
# These fields must be present and non-empty to proceed with project creation/update
mandatory_fields = {
    "project_fields": {
        "project_id": "project_id",
        "project_name": "project_name", 
        "start_date": "start_date",
        "status": "status",
        "cost_center": "cost_center",
        "accounting_area": "accounting_area"
    }
}

def get_missing_field():
    """
    Check for missing mandatory fields and validate dates
    Returns validation status to determine if project should be processed
    """
    not_present_fields = []
    project_data = rail.result("get_project_data")
    
    # Check each mandatory field for presence and non-empty values
    for field in mandatory_fields['project_fields']:
        if project_data.get(field) in [None, '']:
            not_present_fields.append(field)
    
    # Validate date formats and logical order (start < end)
    date_validation = validate_date_format_and_order(project_data)
    
    return {
        'fields': ";".join(not_present_fields),
        'valid_project': bool(not_present_fields) or not date_validation['is_valid'],  # False if any validation fails
        'date_errors': date_validation.get('errors', [])
    }

def check_project_fields():
    """
    Generate validation log properties for missing fields and date errors
    Creates structured log entry for validation failures
    """
    validation_result = get_missing_field()
    project_data = rail.result("get_project_data")
    
    # Collect all validation error details
    details = []
    if validation_result['fields']:
        details.append(f"{validation_result['fields']} not present in the payload")
    
    if validation_result['date_errors']:
        details.extend(validation_result['date_errors'])
    
    return {
        "projectid": project_data.get('project_id', ''),
        "projectname": project_data.get('project_name', ''),
        "clientcode": project_data.get('client_code', ''),
        'action': 'Validation',
        "details": "; ".join(details) if details else "Validation failed",
        "status": 'Exception'
    }


def extract_related_unit(item, unit_type):
    """
    Extract related unit information by type from the payload
    
    Args:
        item: Project data item
        unit_type: Type of unit to extract (Cost Center, Profit Center, Accounting Area)
    
    Returns:
        str: Unit ID if found, None otherwise
    """
    # Validate input structure
    if not item or 'relatedUnit' not in item:
        return None
    
    related_units = item.get('relatedUnit', [])
    if not isinstance(related_units, list):
        return None
    
    # Search for unit by type and return its ID
    for unit in related_units:
        if unit.get('type') == unit_type:
            return unit.get('id')
    
    return None

def extract_related_party_id(item, role):
    """
    Extract related party ID by role from the payload
    
    Args:
        item: Project data item  
        role: Role to extract (End Customer, Project Manager, etc.)
    
    Returns:
        str: Party ID if found, None otherwise
    """
    # Validate input structure  
    if not item or 'relatedParty' not in item:
        return None
    
    related_parties = item.get('relatedParty', [])
    if not isinstance(related_parties, list):
        return None
    
    # Search for party by role and return its ID
    for party in related_parties:
        if party.get('role') == role:
            return party.get('id')
    
    return None

def extract_related_party_name(item, role):
    """
    Extract related party name by role from the payload
    
    Args:
        item: Project data item  
        role: Role to extract (End Customer, Project Manager, etc.)
    
    Returns:
        str: Party name if found, None otherwise
    """
    if not item or 'relatedParty' not in item:
        return None
    
    related_parties = item.get('relatedParty', [])
    if not isinstance(related_parties, list):
        return None
    
    for party in related_parties:
        if party.get('role') == role:
            return party.get('name', party.get('id'))
    
    return None

def get_cost_center_uri(cost_center):
    """
    Get cost center URI and related department information
    Extracts department code from hierarchical path for team assignment mapping
    """
    # Find cost center in department groups by code
    cost_center_details = rail.find_first_by_attr_and_get_attr(
        rail.result("get_cost_center_as_department_groups"), 'code', cost_center
    )

    if not cost_center_details:
        return {
            'uri': None,
            'department_code': None,
            'department_uri': None
        }

    # Extract department code from hierarchical path structure
    # Path format: level1/level2/level3/department_code
    full_path = cost_center_details.get('fullpath_code', '')
    path_parts = full_path.split('/') if full_path else []
    department_code = path_parts[3] if len(path_parts) >= 4 else None

    # Get department URI for team assignment purposes
    department_uri = None
    if department_code:
        department_uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_service_centers_as_department_groups'), 
            'code', department_code, 'uri'
        )

    return {
        'uri': cost_center_details.get('uri'),
        'department_code': department_code,
        'department_uri': department_uri,
    }

def get_org_structure_uri(org_structure):
    """
    Get organization structure URI and legal unit information
    Extracts legal unit code from location hierarchy for OEF mapping
    """
    # Find organization structure in location groups by code
    org_structure_details = rail.find_first_by_attr_and_get_attr(
        rail.result("get_org_structure_as_location_groups"), 'code', org_structure
    )
    
    if not org_structure_details:
        return {
            'uri': None,
            'legal_unit_code': None
        }

    # Extract legal unit code from hierarchical path structure
    # Path format: level1/legal_unit_code/level3/...
    full_path = org_structure_details.get('fullpath_code', '')
    path_parts = full_path.split('/') if full_path else []

    return {
        'uri': org_structure_details.get('uri'),
        'legal_unit_code': path_parts[1] if len(path_parts) > 2 else None,
    }

def normalize_project_data(item, team_assignment_mapper):
    """
    Normalize project data and handle missing references
    Transforms payload data into Replicon-compatible format with proper error handling
    Version 1.7: Added Profit Center and Cost Center extraction from relatedUnit
    """
    exceptions = []  # Track non-fatal issues for logging

    # Parse and normalize date formats from ISO to YYYY-MM-DD
    start_date = parse_iso_date(item.get('start_date')) if item.get('start_date') else None
    end_date = parse_iso_date(item.get('end_date')) if item.get('end_date') else None

    # Map SAP status codes to Replicon status names using configuration
    status_key = item.get('status', '')
    mapped_status = STATUS_MAPPING.get(status_key, 'In Progress')

    # Extract organizational identifiers for Replicon mapping
    # Version 1.4: Apply zero padding transformations for QA testing
    cost_center_raw = item.get('cost_center', '')
    accounting_area_raw = item.get('accounting_area', '')
    delivery_cost_center_raw = item.get('delivery_cost_center', '')

    # Version 1.7: Extract Profit Center and Cost Center codes from relatedUnit (from raw costobject)
    profit_center_raw = item.get('profit_center', '')
    project_cost_center_raw = item.get('project_cost_center', '')

    # Apply padding: Profit Center (10 digits), Accounting Area (4 digits), Cost Center (10 digits)
    cost_center = pad_numeric_code(cost_center_raw, 10)
    accounting_area = pad_numeric_code(accounting_area_raw, 4)
    delivery_cost_center = pad_numeric_code(delivery_cost_center_raw, 10)

    # Version 1.7: Apply padding to Profit Center and Cost Center
    profit_center_code = pad_numeric_code(profit_center_raw, 10)
    project_cost_center_code = pad_numeric_code(project_cost_center_raw, 10)

    # Resolve cost center to Replicon URI and extract department information
    cost_center_details = get_cost_center_uri(cost_center)
    if not cost_center_details['uri'] and cost_center:
        exceptions.append(f"Cost Center '{cost_center}' is not present in Replicon")

    # Resolve accounting area to Replicon URI and extract legal unit
    org_structure_details = get_org_structure_uri(accounting_area)
    if not org_structure_details['uri'] and accounting_area:
        exceptions.append(f"Accounting Area '{accounting_area}' is not present in Replicon")

    # Determine team assignment requirements based on organizational mapping
    # Version 1.7: Pass Profit Center and Cost Center codes for enhanced mapping
    # Pass employee type groups from rail.result (fetched in process_payload)
    team_departments = get_team_departments_for_assignment(
        accounting_area,
        cost_center,
        profit_center_code,
        project_cost_center_code,
        team_assignment_mapper,
        rail.result('get_employee_type_groups')  # Same as other prerequisites
    )
    return {
        'project_code': item.get('project_code', ''),
        'project_name': item.get('project_name', ''),
        'description': item.get('description', ''),
        'start_date': start_date,
        'end_date': end_date,
        'status': mapped_status,

        # cost_center details and URIs
        'cost_center': cost_center,
        'accounting_area': accounting_area,
        'client_code': item.get('client_code', ''),
        'client_name': item.get('client_name', ''),  # Version 1.7: Client name from relatedParty
        'project_manager_id': item.get('project_manager_id', ''),
        'project_manager_permission_set': rail.result('get_project_manager_permission_set'),
        'cost_type': item.get('cost_type', ''),
        'billing_type': item.get('billing_type', ''),
        'time_expense_entry': item.get('time_expense_entry', ''),
        'accounting_group': item.get('accounting_group', ''),
        'project_type': item.get('project_type', ''),
        'control_expert': item.get('control_expert', ''),
        'delivery_cost_center': delivery_cost_center,
        'process_id_group': item.get('process_id_group', ''),
        'project_classification': item.get('contract_type', ''),
        'team_departments': team_departments,
        'main_log': rail.result("create_main_log"),
        'cost_center_uri': cost_center_details['uri'],
        'department_code': cost_center_details['department_code'],
        'department_group_uri': cost_center_details['department_uri'],
        'org_structure_uri': org_structure_details['uri'],
        'project_legal_unit': org_structure_details['legal_unit_code'],
        'profit_center_code': profit_center_code,  # Version 1.7: Profit Center code
        'project_cost_center_code': project_cost_center_code,  # Version 1.7: Cost Center code from relatedUnit
        'exceptions': exceptions,

        'project_oef_fields': rail.result('get_project_oef_fields'),
        'project_dropdown_values': rail.result('get_oef_drop_down_project_oefs')
    }

def pad_numeric_code(value, target_length):
    """
    Pad numeric code with leading zeros to reach target length
    Version 1.4: Data transformation requirement

    Args:
        value: Code value to pad (string or numeric)
        target_length: Desired length after padding

    Returns:
        str: Padded code if numeric, original value if alphanumeric

    Examples:
        pad_numeric_code("370", 4) -> "0370"
        pad_numeric_code("70", 4) -> "0070"
        pad_numeric_code("400912", 10) -> "0000400912"
        pad_numeric_code("T9Q00BCMUV", 10) -> "T9Q00BCMUV" (no change for alphanumeric)
    """
    if not value:
        return value

    # Convert to string if not already
    value_str = str(value).strip()

    # Check if value is purely numeric
    if value_str.isdigit():
        # Pad with leading zeros to target length
        return value_str.zfill(target_length)

    # Return original value for alphanumeric codes
    return value_str

def parse_iso_date(date_string):
    """
    Parse ISO date string to YYYY-MM-DD format
    
    Args:
        date_string: ISO format date string
    
    Returns:
        str: Formatted date string or None
    """
    if not date_string:
        return None
    
    try:
        # Handle ISO format with timezone
        if 'T' in date_string:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(date_string, '%Y-%m-%d')
        
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

def validate_date_format_and_order(project_data):
    """
    Validate date format and order for project dates
    
    Args:
        project_data: Project data dictionary
    
    Returns:
        dict: Validation result with is_valid flag and error details
    """
    errors = []
    start_date_str = project_data.get('start_date')
    end_date_str = project_data.get('end_date')
    
    # Parse and validate start date
    start_date = None
    if start_date_str:
        start_date = parse_iso_date(start_date_str)
        if not start_date:
            errors.append(f"Invalid start date format: {start_date_str}")
    
    # Parse and validate end date
    end_date = None
    if end_date_str:
        end_date = parse_iso_date(end_date_str)
        if not end_date:
            errors.append(f"Invalid end date format: {end_date_str}")
    
    # Critical validation: Ensure start date is before end date
    # This prevents invalid project timelines from being created in Replicon
    if start_date and end_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start_dt > end_dt:
                errors.append(f"Start date ({start_date}) must be before end date ({end_date})")
        except ValueError as e:
            errors.append(f"Date comparison error: {str(e)}")
    
    return {
        'is_valid': len(errors) == 0,
        'errors': errors,
        'parsed_start_date': start_date,
        'parsed_end_date': end_date
    }

def validate_project_dates_against_replicon(payload_data, existing_project_data):
    """
    Validate payload dates against existing project dates in Replicon
    
    Args:
        payload_data: New project data from payload
        existing_project_data: Existing project data from Replicon
    
    Returns:
        dict: Validation result with is_valid flag and error details
    """
    errors = []
    
    if not existing_project_data:
        # No existing project, skip comparison
        return {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
    
    def get_date_from_replicon_date(replicon_date):
        if not replicon_date:
            return None
        return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year']).strftime("%Y-%m-%d")
    
    # Get dates from payload
    payload_start = payload_data.get('start_date')
    payload_end = payload_data.get('end_date')
    
    # Get dates from existing project
    replicon_start = get_date_from_replicon_date(existing_project_data.get('timeEntryDateRange',{}).get('startDate'))
    replicon_end = get_date_from_replicon_date(existing_project_data.get('timeEntryDateRange',{}).get('endDate'))
    
    # Parse payload dates
    if payload_start:
        payload_start = parse_iso_date(payload_start)
        if not payload_start:
            errors.append(f"Invalid payload start date format")
            return {'is_valid': False, 'errors': errors, 'warnings': []}
    
    if payload_end:
        payload_end = parse_iso_date(payload_end)
        if not payload_end:
            errors.append(f"Invalid payload end date format")
            return {'is_valid': False, 'errors': errors, 'warnings': []}
    
    # Parse Replicon dates
    if replicon_start:
        replicon_start = parse_iso_date(replicon_start)
    
    if replicon_end:
        replicon_end = parse_iso_date(replicon_end)
    
    # CRITICAL VALIDATION RULE 1: Prevent timeline overlap conflicts
    # If existing project has end date and payload has start date:
    # Payload start must be before existing end to allow timeline extension
    if replicon_end and payload_start:
        try:
            replicon_end_dt = datetime.strptime(replicon_end, '%Y-%m-%d')
            payload_start_dt = datetime.strptime(payload_start, '%Y-%m-%d')
            
            if payload_start_dt >= replicon_end_dt:
                errors.append(
                    f"project start date - {payload_start} is after the project end date - {replicon_end}"
                )
        except ValueError as e:
            errors.append(f"Date comparison error (Rule 1): {str(e)}")
    
    # CRITICAL VALIDATION RULE 2: Prevent timeline gap/overlap conflicts  
    # If existing project has start date and payload has end date:
    # Payload end must be after existing start to allow timeline updates
    if replicon_start and payload_end:
        try:
            replicon_start_dt = datetime.strptime(replicon_start, '%Y-%m-%d')
            payload_end_dt = datetime.strptime(payload_end, '%Y-%m-%d')
            
            if payload_end_dt <= replicon_start_dt:
                errors.append(
                    f"project end date - {payload_end} is prior to the project start date - {replicon_start}"
                )
        except ValueError as e:
            errors.append(f"Date comparison error (Rule 2): {str(e)}")
    
    return {
        'is_valid': len(errors) == 0,
        'errors': errors,
        'warnings': []
    }

def validate_all_project_dates(project_data, existing_project_data=None):
    """
    Comprehensive project date validation - the main entry point for date validation
    
    Performs two-stage validation:
    1. Format and logical order validation (start < end)  
    2. Compatibility with existing project dates (if updating)
    
    Args:
        project_data: Project data dictionary
        existing_project_data: Existing project data from Replicon (optional)
    
    Returns:
        dict: Combined validation result with is_valid flag and error details
    """
    # STAGE 1: Basic date format and order validation
    # Must pass before proceeding to existing project validation
    format_validation = validate_date_format_and_order(project_data)
    
    if not format_validation['is_valid']:
        return {
            'is_valid': False,
            'validation_type': 'date_format_and_order',
            'errors': format_validation['errors'],
            'warnings': []
        }
    
    # All validations passed
    return {
        'is_valid': True,
        'validation_type': 'all_dates',
        'errors': [],
        'warnings': []
    }

def get_date_validation_error_details():
    """
    Get comprehensive date validation error details for logging
    
    Returns:
        str: Formatted error details
    """
    try:
        validation_result = rail.result('validate_project_dates')
        if validation_result and 'errors' in validation_result:
            return "; ".join(validation_result['errors'])
        return "Date validation failed - unknown error"
    except Exception:
        return "Date validation failed - unable to retrieve error details"

def get_team_departments_for_assignment(accounting_area, cost_center, profit_center_code, project_cost_center_code, team_assignment_mapper, employee_type_groups):
    """
    Get team departments for assignment based on organizational mapping
    Version 1.7: Enhanced with Profit Center and Cost Center codes for mapper lookup

    Determines which service centers should have access to a project based on
    the combination of accounting area, cost center, profit center, and project cost center from the payload.

    Args:
        accounting_area: Accounting area code from payload (Org Structure)
        cost_center: Cost center code from payload (for department extraction)
        profit_center_code: Profit Center code from relatedUnit
        project_cost_center_code: Cost Center code from relatedUnit
        team_assignment_mapper: Mapping configuration with assignment rules
        employee_type_groups: List of employee type groups from Replicon (fetched in process_payload)

    Returns:
        dict: Team assignment information including:
            - uris: List of service center URIs to assign
            - codes: List of department codes
            - should_assign_team: Boolean flag indicating if assignment needed
            - assign_from_department_uris: (Optional) URIs for "Assign team from" restriction
            - assign_from_employee_type_uris: (Optional) Employee type URIs for restriction
            - assign_from_cost_center_uris: (Optional) URIs for Cost Center based "Assign team from" restriction
    """
    if not accounting_area or not cost_center:
        return {
            'uris': [],
            'codes': [],
            'should_assign_team': False
        }

    uris = []
    code_list = []
    assign_from_department_uris = []
    assign_from_employee_types = []
    assign_from_cost_center_uris = []  # Version 1.7: Cost Center based assignment restriction

    # Extract department code from cost center hierarchy
    department = get_cost_center_uri(cost_center)['department_code']
    if not department:
        return {
            'uris': [],
            'codes': [],
            'should_assign_team': False
        }

    # Check TEAM_ASSIGNMENT_MAPPING configuration for matching patterns
    # Version 1.7: Match based on accounting_area, department, profit_center_code, and project_cost_center_code
    mappings = team_assignment_mapper.get(accounting_area, [])
    matched_mapping = None

    for mapping in mappings:
        # Check if department matches
        dept_match = any(dept in department for dept in mapping.get('departments', []))

        # Version 1.7: Check if profit center matches (if specified in mapper)
        profit_center_match = True
        if mapping.get('profit_centers'):
            profit_center_match = profit_center_code in mapping.get('profit_centers', [])

        # Version 1.7: Check if cost center matches (if specified in mapper)
        cost_center_match = True
        if mapping.get('cost_centers'):
            cost_center_match = project_cost_center_code in mapping.get('cost_centers', [])

        # Match if all conditions are satisfied
        if dept_match and profit_center_match and cost_center_match:
            matched_mapping = mapping
            # Only process team_departments if it has actual content
            team_depts = mapping.get('team_departments', '')
            if team_depts:
                # Filter out empty strings from split result
                code_list.extend([c for c in team_depts.split("|") if c])
            break

    if matched_mapping is None:
        fallback_employee_type_uris = []
        for emp_type_name in EXTERNAL_EMPLOYEE_TYPES:
            emp_type_uri = rail.find_first_by_attr_and_get_attr(
                employee_type_groups, 'name', emp_type_name, 'uri'
            )
            if emp_type_uri:
                fallback_employee_type_uris.append(emp_type_uri)
        return {
            'uris': [],
            'codes': [],
            'should_assign_team': False,
            'assign_from_department_uris': [],
            'assign_from_employee_type_uris': fallback_employee_type_uris,
            'assign_from_cost_center_uris': []
        }

    # Convert department codes to Replicon URIs for API calls
    # Only process if we have actual codes (not empty strings)
    for code in code_list:
        if not code:  # Skip empty codes
            continue
        uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_service_centers_as_department_groups'),
            'code', code, 'uri'
        )
        if uri:
            uris.append(uri)

    # Version 1.3: Process assignment restrictions if present
    if matched_mapping:
        # Process "Assign team from" department restrictions
        assign_from_dept_codes = matched_mapping.get('assign_from_departments', '')
        if assign_from_dept_codes:
            # Filter out empty strings
            for dept_code in assign_from_dept_codes.split("|"):
                dept_code = dept_code.strip()
                if not dept_code:  # Skip empty codes
                    continue
                dept_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_service_centers_as_department_groups'),
                    'code', dept_code, 'uri'
                )
                if dept_uri:
                    assign_from_department_uris.append(dept_uri)

        # Version 1.7: Process "Assign team from" Cost Center based restrictions
        assign_from_cost_center_codes = matched_mapping.get('assign_from_cost_centers', '')
        if assign_from_cost_center_codes:
            for cc_code in assign_from_cost_center_codes.split("|"):
                cc_code = cc_code.strip()
                if not cc_code:
                    continue
                # Get Cost Center URI from department groups (same as existing cost center lookup)
                cc_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_cost_center_as_department_groups'),
                    'code', cc_code, 'uri'
                )
                if cc_uri:
                    assign_from_cost_center_uris.append(cc_uri)

        # Process employee type restrictions - convert names to URIs
        # Same pattern as department logic: use parameter passed from normalize_project_data
        employee_types = matched_mapping.get('assign_from_employee_types', '')
        if employee_types:
            # Filter out empty strings
            for emp_type_name in employee_types.split("|"):
                emp_type_name = emp_type_name.strip()
                if not emp_type_name:  # Skip empty names
                    continue
                emp_type_uri = rail.find_first_by_attr_and_get_attr(
                    employee_type_groups,  # Use parameter, not rail.result
                    'name', emp_type_name, 'uri'
                )
                if emp_type_uri:
                    assign_from_employee_types.append(emp_type_uri)

    # Version 1.7: Combine service center URIs and cost center assignment URIs
    # assign_from_cost_center_uris serves both ASSIGNMENT (col 4) and RESTRICTION (col 6) — same value in CSV
    all_assignment_uris = uris + assign_from_cost_center_uris

    return {
        'uris': all_assignment_uris,
        'codes': code_list,
        'should_assign_team': bool(all_assignment_uris),  # True if any assignment URIs found (service centers OR cost centers)
        'assign_from_department_uris': assign_from_department_uris,
        'assign_from_employee_type_uris': assign_from_employee_types,
        'assign_from_cost_center_uris': assign_from_cost_center_uris  # Version 1.7: used for RESTRICTION scope
    }
    
def format_integration_logs(dag_run):
    """
    Format and categorize integration logs for final reporting
    Counts different log types for email summary and monitoring
    """
    final_log_records = rail.load_all_records(dag_run.conf['main_log'])
    
    # Count records by status type for summary statistics
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'exception', final_log_records))))
    
    return final_log_records

def generate_unique_work_id():
    """Generate unique work ID for API requests"""
    return str(uuid.uuid4())



def get_success_or_exception_logs(dag_run):
    """
    Generate success/exception log properties for project processing completion
    Distinguishes between full success and partial success with warnings
    """
    exceptions = dag_run.conf.get('exceptions', [])
    action = 'Create' if not request_payload.does_wbs_exist() else 'Update'

    if rail.result("is_project_manager_valid") == 'create_or_update_project':
        exceptions.append("Project Manager is not present in Replicon.")
    
    if rail.result("is_project_manager_present") == 'create_or_update_project':
        exceptions.append("Project Manager is not available in payload")

    # Determine base success message based on operation type
    msg = "project created successfully" if not request_payload.does_wbs_exist() else "project updated successfully"
    check = False
    
    # If there were reference resolution issues, mark as partial success
    if exceptions:
        msg = f'project {action.lower()}d partially - ' + ', '.join(exceptions)
        check = True
    
    return {
        'projectid': dag_run.conf['project_code'],
        'projectname': dag_run.conf['project_name'],
        'clientcode': dag_run.conf.get('client_code', ''),
        'action': action,
        'details': msg,
        'status': 'Exception' if check else "Success"  # Exception = partial success with warnings
    }

def parse_and_transform_api_response_to_project_list(response_text, operation_type):
    """
    Single task to parse API response and transform to project list format.
    Combines parsing, validation, and transformation in one operation.

    Args:
        response_text: Raw API response with concatenated JSON objects
        operation_type: "create" or "update"

    Returns:
        list: Standardized project records list
    """
    if not response_text or not response_text.strip():
        print(f"No {operation_type} data received from API")
        return []

    # Parse concatenated JSON objects
    json_objects = []
    decoder = json.JSONDecoder()
    response_text = response_text.strip()

    while response_text:
        response_text = response_text.lstrip()
        if not response_text:
            break

        try:
            obj, end_idx = decoder.raw_decode(response_text)
            json_objects.append(obj)
            response_text = response_text[end_idx:].lstrip()
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {str(e)}")
            break

    print(f"Parsed {len(json_objects)} {operation_type} events")

    if not json_objects:
        return []

    # Transform to project list format
    project_list = []

    for event_obj in json_objects:
        try:
            # Validate event structure and extract costobject
            if not (isinstance(event_obj, dict) and
                   'data' in event_obj and
                   isinstance(event_obj['data'], dict) and
                   'costobject' in event_obj['data']):
                continue

            costobject = event_obj['data']['costobject']

            # Validate costobject has required fields
            if not (isinstance(costobject, dict) and 'id' in costobject):
                continue

            # Create standardized project record
            project_record = {
                "project_id": costobject.get('id', ''),
                "project_name": fix_text_encoding(costobject.get('name', '')),
                "description": fix_text_encoding(costobject.get('description', '')),
                "start_date": costobject.get("validFor", {}).get("startDateTime", ""),
                "end_date": costobject.get("validFor", {}).get("endDateTime", ""),
                "status": costobject.get('status', '').get('sourceKey', ''),
                "cost_center": extract_related_unit(costobject, "Profit Center"),
                "accounting_area": extract_related_unit(costobject, "Accounting Area"),
                "profit_center": extract_related_unit(costobject, "Profit Center"),  # Version 1.7: Explicit Profit Center
                "project_cost_center": extract_related_unit(costobject, "Cost Center"),  # Version 1.7: Cost Center from relatedUnit
                "client_code": extract_related_party_id(costobject, "End Customer"),
                "client_name": fix_text_encoding(extract_related_party_name(costobject, "End Customer")),  # Version 1.7: Client name from relatedParty
                "project_manager_id": extract_related_party_id(costobject, "Project Manager"),
                "billing_type": costobject.get("billingDetail", {}).get("billingType", ""),
                "cost_type": costobject.get("billingDetail", {}).get("costType", ""),
                "time_expense_entry": costobject.get("billingDetail", {}).get("timeExpenseEntry", ""),
                "accounting_group": costobject.get("billingDetail", {}).get("settlementGroup", ""),
                "project_type": costobject.get("category", {}).get("sourceKey", ""),
                "control_expert": costobject.get("category", {}).get("sourceControl", ""),
                "process_id_group": event_obj.get('source', ""),
                "delivery_cost_center": extract_related_unit(costobject, "Cost Center"),
                "contract_type": costobject.get("contractType", "")
            }

            project_list.append(project_record)

        except Exception as e:
            continue

    return project_list

def handle_api_error_504(operation_type):
    """
    Handle 503 Service Unavailable errors as success (no data available)

    Args:
        operation_type: "create" or "update" operation type

    Returns:
        dict: Success status for 504 errors, failure status for others
    """
    if rail.result(f'fetch_{operation_type}_projects'):
        return {
            'process': True,
            'message': 'API is Processed Successfully',
            'status_code': 200
        }
    try:
        fetch_result = rail.result(f'fetch_{operation_type}_projects', 'error')

        status_code = fetch_result.get('status_code', 0)
        error_message = fetch_result.get('exc_message', '')

        if status_code == 500 and "504:Gateway Timeout" in error_message:
            return {
                'process': False,
                'message': '504:Gateway Timeout',
                'status_code': 504
            }
        else:
            return {
                'message': error_message,
                'process': False,
                'status_code': status_code
            }
    except Exception as e:
        return {
            'message': f'{str(e)}',
            'process': False,
            'status_code': rail.result(f'fetch_{operation_type}_projects', 'error').get('status_code', 0) if rail.result(f'fetch_{operation_type}_projects') else 504
        }

def validate_integration_success():
    """
    Validate the overall integration success by checking API status results.
    Fails the DAG if either API had genuine errors (not 503 or success).

    Returns:
        dict: Integration status summary
    """
    create_status = rail.result('get_create_projects_api_status')['status_code']

    if create_status not in [200, 504]:
        raise Exception("Create Projects API encountered an error.")

    # Check update API status
    update_status = rail.result('get_update_projects_api_status')['status_code']
    if update_status not in [200, 504]:
        raise Exception("Update Projects API encountered an error.")

    return {
        'message': 'Integration completed successfully.'
    }
