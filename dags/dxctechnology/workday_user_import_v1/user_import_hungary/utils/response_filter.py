import json
from datetime import datetime
from rail import smartjoin_by_delim, set_result, find_first_by_attr_and_get_attr
from airflow.exceptions import AirflowException

LOCATION_DELIMITER = " | "

def get_starting_balance_script_data_handler(response):
    script_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'Starting Balance Set To', 'uri', None)
    if script_uri:
        return script_uri
    raise AirflowException("Script `Starting Balance Set To` is not found")


def get_prevent_balance_overdraw_script_data_handler(response):
    script_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'Prevent balance overdraw', 'uri', None)
    if script_uri:
        return script_uri
    raise AirflowException("Script `Prevent balance overdraw` is not found")

def get_all_user_custom_fields_data_handler(config, response):
    UDF_FIELDS = config.UDFs.copy()
    res = {}
    set_result(key= "response", val = response)
    # doing in for loop to avoid multiple iter of response while using rail.find_first_by_attr_and_get_attr
    for udf in response:
        if not UDF_FIELDS:
            break
        if udf['displayText'] in UDF_FIELDS:
            res[udf['displayText'].replace(
                ".", "").replace(" ", "_").lower()] = {"name": udf['displayText'], "uri": udf['uri']}
            UDF_FIELDS.remove(udf['displayText'])
    set_result(key = "udfs_not_found", val=UDF_FIELDS)
    return res

def get_value(data, index, pluck_key):
    return data[index].get(pluck_key)

def get_location_response_filter(response):
    return list(map(lambda location: {
        "name": get_value(location['cells'] , 0, 'textValue'),
        "uri": get_value(location['cells'] , 0, 'uri'),
        "fullpath": smartjoin_by_delim([location['textValue'] for location in get_value(location['cells'] , 1, 'cellCollection')],
                            LOCATION_DELIMITER)
    }, response['rows']))

def get_employeegroup_response_filter(response):

    employee_data = list(map(lambda employee_type: {
            "name": get_value(employee_type['cells'], 0, 'textValue'),
            "uri": get_value(employee_type['cells'], 0, 'uri'),
            "full_path": smartjoin_by_delim([employee_type['textValue'] for employee_type in get_value(employee_type['cells'] , 1, 'cellCollection')],
                            LOCATION_DELIMITER),
            "contractor": "Yes" if "contractor" in get_value(employee_type['cells'], 0, 'textValue').lower() else "No"

        }, response['rows']))

    return {
        "employee_data": employee_data,
        "employee_data_for_assignment": list(filter(lambda item: item['contractor'].lower()=="no" ,employee_data))
    }

def get_companycode_response_filter(response):
    return list(map(lambda company_code: {
            "name": get_value(company_code['cells'], 0, 'textValue'),
            "uri": get_value(company_code['cells'], 0, 'uri'),
            "full_path": smartjoin_by_delim([_company_code['textValue'] for _company_code in get_value(company_code['cells'] , 1, 'cellCollection')],
                            LOCATION_DELIMITER),
            "parent":  get_value(company_code['cells'] , 1, 'cellCollection')[0]['textValue']

        }, response['rows']))

def filter_user_response(response):
    if not response:
        return None
    
    # Extract relevant fields from response
    filtered = {
        'uri': response.get('uri'),
        'employeeId': response.get('employeeId'),
        'email': response.get('email'),
        'firstName': response.get('firstName'),
        'lastName': response.get('lastName'),
        'isEnabled': response.get('isEnabled'),
        'userDetails': filter_user_details(response.get('userDetails', {})),
        'supervisor': filter_supervisor_info(response.get('supervisor', {})),
        'customFields': response.get('customFields', {})
    }
    
    return filtered


def filter_user_details(details):
    if not details:
        return {}
    
    return {
        'hireDate': details.get('hireDate'),
        'terminationDate': details.get('terminationDate'),
        'employmentStatus': details.get('employmentStatus'),
        'jobTitle': details.get('jobTitle'),
        'department': details.get('department'),
        'costCenter': details.get('costCenter'),
        'location': details.get('location'),
        'companyCode': details.get('companyCode'),
        'employeeType': details.get('employeeType'),
        'ftePercentage': details.get('ftePercentage')
    }


def filter_supervisor_info(supervisor):
    if not supervisor:
        return {}
    
    return {
        'uri': supervisor.get('uri'),
        'employeeId': supervisor.get('employeeId'),
        'name': f"{supervisor.get('firstName', '')} {supervisor.get('lastName', '')}".strip(),
        'email': supervisor.get('email')
    }


def filter_timeoff_response(response):
    if not response:
        return []
    
    filtered = []
    for assignment in response:
        filtered.append({
            'uri': assignment.get('uri'),
            'timeOffTypeUri': assignment.get('timeOffTypeUri'),
            'timeOffTypeName': assignment.get('timeOffTypeName'),
            'effectiveDate': assignment.get('effectiveDate'),
            'expiryDate': assignment.get('expiryDate'),
            'balance': assignment.get('balance'),
            'accrualRate': assignment.get('accrualRate'),
            'isActive': assignment.get('isActive', True)
        })
    
    return filtered


def filter_schedule_response(response):
    if not response:
        return {}
    
    return {
        'scheduleType': response.get('scheduleType'),
        'startTime': response.get('startTime'),
        'endTime': response.get('endTime'),
        'breakDuration': response.get('breakDuration'),
        'workDays': response.get('workDays', []),
        'shiftPattern': response.get('shiftPattern'),
        'isFlexible': response.get('isFlexible', False)
    }


def filter_product_assignment_response(response):
    if not response:
        return []
    
    filtered = []
    for product in response:
        filtered.append({
            'productUri': product.get('productUri'),
            'productName': product.get('productName'),
            'enabled': product.get('enabled'),
            'accessLevel': product.get('accessLevel'),
            'assignedDate': product.get('assignedDate')
        })
    
    return filtered


def filter_error_response(response):
    if not response:
        return {'error': 'Unknown error', 'code': 'UNKNOWN'}
    
    # Handle different error response formats
    if isinstance(response, dict):
        if 'error' in response:
            return {
                'error': response.get('error'),
                'code': response.get('errorCode', 'API_ERROR'),
                'details': response.get('errorDetails', ''),
                'field': response.get('field', '')
            }
        elif 'message' in response:
            return {
                'error': response.get('message'),
                'code': response.get('code', 'API_ERROR'),
                'details': response.get('details', '')
            }
    
    # If response is a string, return as error message
    if isinstance(response, str):
        return {
            'error': response,
            'code': 'API_ERROR'
        }
    
    return {'error': 'Unexpected error format', 'code': 'PARSE_ERROR'}


def filter_bulk_response(responses):
    results = {
        'success': [],
        'failed': [],
        'total': len(responses) if responses else 0
    }
    
    for response in responses:
        if response.get('success'):
            results['success'].append({
                'employeeId': response.get('employeeId'),
                'uri': response.get('uri'),
                'action': response.get('action')
            })
        else:
            results['failed'].append({
                'employeeId': response.get('employeeId'),
                'error': filter_error_response(response.get('error'))
            })
    
    return results


def filter_log_response(response):
    if not response:
        return {}
    
    return {
        'logId': response.get('logId'),
        'createdAt': response.get('createdAt'),
        'severity': response.get('severity'),
        'message': response.get('message'),
        'properties': response.get('properties', {}),
        'dagRunId': response.get('dagRunId'),
        'taskId': response.get('taskId')
    }


def filter_collection_response(response):
    if not response:
        return {}
    
    return {
        'collectionName': response.get('name'),
        'recordCount': response.get('count', 0),
        'columns': response.get('columns', []),
        'created': response.get('created'),
        'filtered': response.get('filtered', False)
    }


def filter_validation_response(response):
    if not response:
        return {'valid': False, 'errors': []}
    
    return {
        'valid': response.get('isValid', False),
        'errors': response.get('validationErrors', []),
        'warnings': response.get('validationWarnings', []),
        'fieldErrors': response.get('fieldErrors', {})
    }


def extract_user_uri(response):
    if not response:
        return None
    
    # Direct URI field
    if 'uri' in response:
        return response['uri']
    
    # User object
    if 'user' in response and 'uri' in response['user']:
        return response['user']['uri']
    
    # UserDetails object
    if 'userDetails' in response and 'uri' in response['userDetails']:
        return response['userDetails']['uri']
    
    return None


def extract_success_status(response):
    if not response:
        return False
    
    # Check various success indicators
    if isinstance(response, dict):
        if 'success' in response:
            return response['success']
        if 'isSuccess' in response:
            return response['isSuccess']
        if 'status' in response:
            return response['status'].lower() in ['success', 'completed', 'ok']
        if 'error' in response or 'errorCode' in response:
            return False
    
    # If response exists and no error indicators, assume success
    return True


def format_date_response(date_str):
    if not date_str:
        return None
    
    # Try various date formats
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d'
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    
    return date_str


def aggregate_responses(responses):
    aggregated = {
        'total': len(responses) if responses else 0,
        'successful': 0,
        'failed': 0,
        'results': [],
        'errors': []
    }
    
    for response in responses:
        if extract_success_status(response):
            aggregated['successful'] += 1
            aggregated['results'].append(response)
        else:
            aggregated['failed'] += 1
            aggregated['errors'].append(filter_error_response(response))
    
    return aggregated


def sanitize_response(response):
    if not response:
        return response
    
    sensitive_fields = [
        'password', 'token', 'apiKey', 'secret',
        'ssn', 'socialSecurityNumber', 'taxId'
    ]
    
    if isinstance(response, dict):
        sanitized = response.copy()
        for field in sensitive_fields:
            if field in sanitized:
                sanitized[field] = '***REDACTED***'
            # Check nested fields
            for key, value in sanitized.items():
                if isinstance(value, dict) and field in value:
                    sanitized[key][field] = '***REDACTED***'
        return sanitized
    
    return response

def get_effective_grp_with_disabled_assigned_grp_handler(_data, grp_key, sub_grp_key, list_item_index=0):
    if not _data:
        return {}
    
    if not _data[list_item_index]:
        return {}
    
    if not _data[list_item_index][grp_key]:
        return {}

    if not _data[list_item_index][grp_key][sub_grp_key]:
        return {}

    return _data[list_item_index][grp_key][sub_grp_key]

def get_effective_grp_membership_data_handler(response):
    return_data = {}
    set_result(key="response", val=response)
    return_data['costCenter'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['costCenters'],
        grp_key = 'costCenter',
        sub_grp_key = 'costCenter',
        list_item_index = 0
    ) if response['costCenters'] else {})

    return_data['department'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['departments'],
        grp_key = 'department',
        sub_grp_key = 'department',
        list_item_index = 0
    ) if response['departments'] else {})

    return_data['division'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['divisions'],
        grp_key = 'division',
        sub_grp_key = 'division',
        list_item_index = 0
    ) if response['divisions'] else {})

    return_data['employeeType'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['employeeTypes'],
        grp_key = 'employeeType',
        sub_grp_key = 'employeeType',
        list_item_index = 0
    ) if response['employeeTypes'] else {})

    return_data['location'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['locations'],
        grp_key = 'location',
        sub_grp_key = 'location',
        list_item_index = 0
    ) if response['locations'] else {})

    return_data['serviceCenter'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['serviceCenters'],
        grp_key = 'serviceCenter',
        sub_grp_key = 'serviceCenter',
        list_item_index = 0
    ) if response['serviceCenters'] else {})

    return_data['parent_location'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['locations'],
        grp_key = 'location',
        sub_grp_key = 'parent',
        list_item_index = 0
    ) if response['locations'] else {})

    return_data['parent_division'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['divisions'],
        grp_key = 'division',
        sub_grp_key = 'parent',
        list_item_index = 0
    ) if response['divisions'] else {})

    return return_data
