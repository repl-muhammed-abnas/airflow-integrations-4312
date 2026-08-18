import uuid
import json
from datetime import datetime
import rail
from tsystems.project_import_file_based.config import STATUS_MAPPING
from tsystems.project_import_file_based.utils import request_payload

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

def get_missing_mandatory_fields_message(item):
    """Generate detailed error message listing all missing mandatory fields"""
    field_labels = {
        'project_code': 'Code',
        'project_name': 'Name',
        'start_date': 'Start Date',
        'status': 'Status',
        'cost_center': 'Cost Center',
        'accounting_area': 'Org Structure'
    }

    missing_fields = []
    for field_key, field_name in field_labels.items():
        if not item.get(field_key, '').strip():
            missing_fields.append(field_name)

    if missing_fields:
        return f"{', '.join(missing_fields)} not present in the input payload"
    return "Mandatory field is missing"

def get_missing_field():
    """Check for missing mandatory fields and validate dates"""
    not_present_fields = []
    project_data = rail.result("get_project_data")
    for field in mandatory_fields['project_fields']:
        if project_data.get(field) in [None, '']:
            not_present_fields.append(field)
    date_validation = validate_date_format_and_order(project_data)
    
    return {
        'fields': ";".join(not_present_fields),
        'valid_project': bool(not_present_fields) or not date_validation['is_valid'],  # False if any validation fails
        'date_errors': date_validation.get('errors', [])
    }

def check_project_fields():
    """Generate validation log properties for missing fields and date errors"""
    validation_result = get_missing_field()
    project_data = rail.result("get_project_data")
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
    """Extract related unit information by type from the payload"""
    if not item or 'relatedUnit' not in item:
        return None
    
    related_units = item.get('relatedUnit', [])
    if not isinstance(related_units, list):
        return None
    
    for unit in related_units:
        if unit.get('type') == unit_type:
            return unit.get('id')
    
    return None

def extract_related_party_id(item, role):
    """Extract related party ID by role from the payload"""
    if not item or 'relatedParty' not in item:
        return None
    
    related_parties = item.get('relatedParty', [])
    if not isinstance(related_parties, list):
        return None
    
    for party in related_parties:
        if party.get('role') == role:
            return party.get('id')
    
    return None

def extract_related_party_name(item, role):
    """Extract related party name by role from the payload"""
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
    """Get cost center URI and related department information"""
    cost_center_details = rail.find_first_by_attr_and_get_attr(
        rail.result("get_cost_center_as_department_groups"), 'code', cost_center
    )

    if not cost_center_details:
        return {
            'uri': None,
            'department_code': None,
            'department_uri': None
        }

    full_path = cost_center_details.get('fullpath_code', '')
    path_parts = full_path.split('/') if full_path else []
    department_code = path_parts[3] if len(path_parts) >= 4 else None

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
    """Get organization structure URI and legal unit information"""
    org_structure_details = rail.find_first_by_attr_and_get_attr(
        rail.result("get_org_structure_as_location_groups"), 'code', org_structure
    )

    if not org_structure_details:
        return {
            'uri': None,
            'legal_unit_code': None
        }

    full_path = org_structure_details.get('fullpath_code', '')
    path_parts = full_path.split('/') if full_path else []

    return {
        'uri': org_structure_details.get('uri'),
        'legal_unit_code': path_parts[1] if len(path_parts) > 2 else None,
    }

def normalize_project_data(item, team_assignment_mapper):
    """Normalize project data and handle missing references"""
    exceptions = []
    start_date = parse_iso_date(item.get('start_date')) if item.get('start_date') else None
    end_date = parse_iso_date(item.get('end_date')) if item.get('end_date') else None

    status_key = item.get('status', '')
    mapped_status = STATUS_MAPPING.get(status_key, 'In Progress')

    cost_center_raw = item.get('cost_center', '')
    accounting_area_raw = item.get('accounting_area', '')
    delivery_cost_center_raw = item.get('delivery_cost_center', '')

    cost_center = pad_numeric_code(cost_center_raw, 10)
    accounting_area = pad_numeric_code(accounting_area_raw, 4)
    delivery_cost_center = pad_numeric_code(delivery_cost_center_raw, 10)

    cost_center_details = get_cost_center_uri(cost_center)
    if not cost_center_details['uri'] and cost_center:
        exceptions.append(f"Cost Center '{cost_center}' is not present in Replicon")

    org_structure_details = get_org_structure_uri(accounting_area)
    if not org_structure_details['uri'] and accounting_area:
        exceptions.append(f"Accounting Area '{accounting_area}' is not present in Replicon")

    team_departments = get_team_departments_for_assignment(
        accounting_area,
        cost_center,
        team_assignment_mapper,
        rail.result('get_employee_type_groups')
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
        'exceptions': exceptions,

        'project_oef_fields': rail.result('get_project_oef_fields'),
        'project_dropdown_values': rail.result('get_oef_drop_down_project_oefs')
    }

def pad_numeric_code(value, target_length):
    """Pad numeric code with leading zeros to reach target length"""
    if not value:
        return value

    value_str = str(value).strip()

    if value_str.isdigit():
        return value_str.zfill(target_length)

    return value_str

def parse_iso_date(date_string):
    """Parse ISO date string to YYYY-MM-DD format"""
    if not date_string:
        return None
    
    try:
        if 'T' in date_string:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(date_string, '%Y-%m-%d')

        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

def validate_date_format_and_order(project_data):
    """Validate date format and order for project dates"""
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
    """Validate payload dates against existing project dates in Replicon"""
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
    """Comprehensive project date validation"""
    format_validation = validate_date_format_and_order(project_data)
    
    if not format_validation['is_valid']:
        return {
            'is_valid': False,
            'validation_type': 'date_format_and_order',
            'errors': format_validation['errors'],
            'warnings': []
        }
    
    # STAGE 2: Advanced validation against existing project timeline
    # Only applies when updating existing projects
    if existing_project_data:
        replicon_validation = validate_project_dates_against_replicon(
            project_data, existing_project_data
        )
        
        if not replicon_validation['is_valid']:
            return {
                'is_valid': False,
                'validation_type': 'existing_project_dates',
                'errors': replicon_validation['errors'],
                'warnings': replicon_validation.get('warnings', [])
            }
    
    # All validations passed
    return {
        'is_valid': True,
        'validation_type': 'all_dates',
        'errors': [],
        'warnings': []
    }

def get_date_validation_error_details():
    """Get comprehensive date validation error details for logging"""
    try:
        validation_result = rail.result('validate_project_dates')
        if validation_result and 'errors' in validation_result:
            return "; ".join(validation_result['errors'])
        return "Date validation failed - unknown error"
    except Exception:
        return "Date validation failed - unable to retrieve error details"

def get_team_departments_for_assignment(accounting_area, cost_center, team_assignment_mapper, employee_type_groups):
    """Get team departments for assignment based on organizational mapping"""
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

    department = get_cost_center_uri(cost_center)['department_code']
    if not department:
        return {
            'uris': [],
            'codes': [],
            'should_assign_team': False
        }
    mappings = team_assignment_mapper.get(accounting_area, [])
    matched_mapping = None

    for mapping in mappings:
        if any(dept in department for dept in mapping.get('departments', [])):
            matched_mapping = mapping
            team_depts = mapping.get('team_departments', '')
            if team_depts:
                code_list.extend([c for c in team_depts.split("|") if c])
            break

    for code in code_list:
        if not code:
            continue
        uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_service_centers_as_department_groups'),
            'code', code, 'uri'
        )
        if uri:
            uris.append(uri)

    if matched_mapping:
        assign_from_dept_codes = matched_mapping.get('assign_from_departments', '')
        if assign_from_dept_codes:
            for dept_code in assign_from_dept_codes.split("|"):
                dept_code = dept_code.strip()
                if not dept_code:
                    continue
                dept_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_service_centers_as_department_groups'),
                    'code', dept_code, 'uri'
                )
                if dept_uri:
                    assign_from_department_uris.append(dept_uri)

        employee_types = matched_mapping.get('assign_from_employee_types', '')
        if employee_types:
            for emp_type_name in employee_types.split("|"):
                emp_type_name = emp_type_name.strip()
                if not emp_type_name:
                    continue
                emp_type_uri = rail.find_first_by_attr_and_get_attr(
                    employee_type_groups,
                    'name', emp_type_name, 'uri'
                )
                if emp_type_uri:
                    assign_from_employee_types.append(emp_type_uri)

    return {
        'uris': uris,
        'codes': code_list,
        'should_assign_team': bool(uris),  # Only assign if URIs found
        'assign_from_department_uris': assign_from_department_uris,
        'assign_from_employee_type_uris': assign_from_employee_types
    }
    
def format_integration_logs(dag_run):
    """Format and categorize integration logs for final reporting"""
    final_log_records = rail.load_all_records(dag_run.conf['main_log'])
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
    """Generate success/exception log properties for project processing completion"""
    exceptions = dag_run.conf.get('exceptions', [])
    action = 'Create' if not request_payload.does_wbs_exist() else 'Update'

    if rail.result("is_project_manager_valid") == 'create_or_update_project':
        exceptions.append("Project Manager is not present in Replicon.")
    
    if rail.result("is_project_manager_present") == 'create_or_update_project':
        exceptions.append("Project Manager is not available in payload")

    msg = "project created successfully" if not request_payload.does_wbs_exist() else "project updated successfully"
    check = False
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
    """Parse API response and transform to project list format"""
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
                "project_name": costobject.get('name', ''),
                "description": costobject.get('description', ''),
                "start_date": costobject.get("validFor", {}).get("startDateTime", ""),
                "end_date": costobject.get("validFor", {}).get("endDateTime", ""),
                "status": costobject.get('status', '').get('sourceKey', ''),
                "cost_center": extract_related_unit(costobject, "Profit Center"),
                "accounting_area": extract_related_unit(costobject, "Accounting Area"),
                "client_code": extract_related_party_id(costobject, "End Customer"),
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

def parse_sftp_json_file_to_project_list():
    """Parse JSON file from SFTP and transform to project list format"""
    try:
        # Download and read the file content
        with rail.existing_artifact(rail.result("download_file")) as artifact:
            with open(artifact.local_filename, 'r', encoding='utf-8') as f:
                file_content = f.read()
    except Exception as e:
        print(f"Error reading JSON file from SFTP: {str(e)}")
        return []

    if not file_content or not file_content.strip():
        print("Empty JSON file received from SFTP")
        return []

    # Parse JSON content
    try:
        json_data = json.loads(file_content)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return []

    # Handle both single object and array of objects
    if isinstance(json_data, dict):
        json_objects = [json_data]
    elif isinstance(json_data, list):
        json_objects = json_data
    else:
        print(f"Unexpected JSON structure: {type(json_data)}")
        return []

    print(f"Parsed {len(json_objects)} project records from SFTP file")

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
                "project_name": costobject.get('name', ''),
                "description": costobject.get('description', ''),
                "start_date": costobject.get("validFor", {}).get("startDateTime", ""),
                "end_date": costobject.get("validFor", {}).get("endDateTime", ""),
                "status": costobject.get('status', '').get('sourceKey', ''),
                "cost_center": extract_related_unit(costobject, "Profit Center"),
                "accounting_area": extract_related_unit(costobject, "Accounting Area"),
                "client_code": extract_related_party_id(costobject, "End Customer"),
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
            print(f"Error parsing project record: {str(e)}")
            continue

    print(f"Successfully transformed {len(project_list)} project records")
    return project_list

def handle_api_error_504(operation_type):
    """Handle 503 Service Unavailable errors as success"""
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
    """Validate the overall integration success by checking API status results"""
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
