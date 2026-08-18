import json
from airflow.exceptions import AirflowException
import rail

null = None


def get_current_group_membership(response):
    def safe_get_nested_value(array_key, item_key, field):
        """Safely extract nested values from group membership arrays"""
        if not response:
            return null
            
        array = response.get(array_key)
        if not array:
            return null
        
        first_item = array[0] if array and len(array) > 0 else null
        if not first_item:
            return null
            
        nested_item = first_item.get(item_key) if first_item else null
        if not nested_item:
            return null
            
        final_item = nested_item.get(item_key) if nested_item else null
        if not final_item:
            return null
            
        return final_item.get(field) if final_item else null
    
    return {
        "existinglocationuri": safe_get_nested_value('locations', 'location', 'uri'),
        "existinglocationname": safe_get_nested_value('locations', 'location', 'displayText'),
        "existingservicecenteruri": safe_get_nested_value('serviceCenters', 'serviceCenter', 'uri'),
        "existingservicecentername": safe_get_nested_value('serviceCenters', 'serviceCenter', 'displayText'),
        "existingdepartmenturi": safe_get_nested_value('departments', 'department', 'uri'),
        "existingdepartmentname": safe_get_nested_value('departments', 'department', 'displayText'),
        "existingcostcenteruri": safe_get_nested_value('costCenters', 'costCenter', 'uri'),
        "existingcostcentername": safe_get_nested_value('costCenters', 'costCenter', 'displayText'),
        "existingemployeetypeuri": safe_get_nested_value('employeeTypes', 'employeeType', 'uri'),
        "existingemployeetypename": safe_get_nested_value('employeeTypes', 'employeeType', 'displayText')
    }

def get_required_holiday_calendars(response, assignment_rules_mapper_data):
    """
    Extracts required holiday calendars from the response based on the assignment rules mapper data.
    Returns name:uri format.
    """
    # Get all unique holiday calendars from mapper
    unique_holiday_calendars = set()
    for rule in assignment_rules_mapper_data:
        calendar_name = rule.get('holiday_calendar')
        if calendar_name:
            unique_holiday_calendars.add(calendar_name)
    
    # Get only those holiday calendars from response in name:uri format
    holiday_calendars = {}
    for calendar in response:
        if calendar['displayText'] in unique_holiday_calendars:
            holiday_calendars[calendar['displayText']] = calendar['uri']
    
    return holiday_calendars

def get_all_oef_tags(response):
    return [{
        "oef_tag": oef_data["name"],
        "code": oef_data["code"],
        "description": oef_data["description"],
        "is_enabled": oef_data["isEnabled"],
        "uri": oef_data["uri"]
    } for oef_data in response["tags"]]

def get_required_timeoffs_data(response, time_off_type_mapper_data):
    """
    Extracts required time off types from the response based on the iPipeline mapper data.
    Returns name:uri format.
    """
    # Get all unique time off types from mapper
    unique_timeoff_types = set()
    for policy in time_off_type_mapper_data:
        timeoff_type_name = policy.get('time_off_type')
        if timeoff_type_name:
            unique_timeoff_types.add(timeoff_type_name)
    
    # Get only those time off types from response in name:uri format
    time_off_types = {}
    for timeoff_type in response:
        if timeoff_type['displayText'] in unique_timeoff_types:
            time_off_types[timeoff_type['displayText']] = timeoff_type['uri']
    
    return time_off_types

def get_required_permissions_data(response, permissions_mapper_data, defaults_mapper_data):
    """
    Extracts required permissions from the response based on the iPipeline mapper data.
    Returns name:uri format.
    """
    default_permission = defaults_mapper_data.get("default_permission")
    supervisor_permission = defaults_mapper_data.get("supervisor_permission")
    schedule_manager_supervisor_permission = defaults_mapper_data.get("schedule_manager_supervisor_permission")
    schedule_manager_not_supervisor_permission = defaults_mapper_data.get("schedule_manager_not_supervisor_permission")
    
    # Get all unique permissions from mapper
    unique_permissions = set()
    for title, permissions_list in permissions_mapper_data.items():
        unique_permissions.update(permissions_list)
    
    # Add default mapper permissions to unique_permissions set
    unique_permissions.add(schedule_manager_supervisor_permission)
    unique_permissions.add(schedule_manager_not_supervisor_permission)
    unique_permissions.add(supervisor_permission)
    unique_permissions.add(default_permission)
    
    # Get only those permissions from response in name:uri format
    permission_set = {}
    for permission in response:
        if permission['displayText'] in unique_permissions:
            permission_set[permission['displayText']] = permission['uri']
    
    return permission_set

def get_required_timesheet_templates_data(response, assignment_rules_mapper_data):
    """
    Extracts required timesheet templates from the response based on the iPipeline mapper data.
    Returns name:uri format.
    """
    # Get all unique timesheet templates from mapper
    unique_timesheet_templates = set()
    
    for rule in assignment_rules_mapper_data:
        template_name = rule.get('timesheet_template')
        if template_name:  # Skip empty templates
            unique_timesheet_templates.add(template_name)
    
    # Get only those timesheet templates from response in name:uri format
    timesheet_templates = {}
    for template in response:
        if template['displayText'] in unique_timesheet_templates:
            timesheet_templates[template['displayText']] = template['uri']
    
    return timesheet_templates

def get_required_activities(response, assignment_rules_mapper_data):
    """
    Extracts required activities from the response based on the assignment rules mapper data.
    Returns name:uri format.
    """
    # Get all unique activities from mapper
    unique_activities = set()
    for rule in assignment_rules_mapper_data:
        activities_list = rule.get('activities', [])
        if activities_list:
            unique_activities.update(activities_list)
    
    # Get only those activities from response in name:uri format
    activities = {}
    for activity in response:
        if activity['displayText'] in unique_activities:
            activities[activity['displayText']] = activity['uri']
    
    return activities

def get_existing_details_of_group(response):
    """
    Parse Replicon list service response into structured group data
    Extracts name, URI, full path hierarchy, and codes from group responses
    """
    if not response:
        return []
        
    return [{
        'name': group['cells'][0].get('textValue'),
        'uri': group['cells'][0].get('uri'),
        'fullpath': rail.smartjoin_by_delim([item['textValue'] for item in group['cells'][1]['cellCollection']], '/'),
        'fullpath_code': rail.smartjoin_by_delim([item['textValue'] for item in group['cells'][2]['cellCollection']], '/'),
        'length': len([item['textValue'] for item in group['cells'][1]['cellCollection']]),
        'code': group['cells'][3].get('textValue', null),
    } for group in response['rows']]

def get_user_current_holiday_calendar(response):
    if not response:
        return {}
    return {
        "holiday_calendar_name": response[0]["holidayCalendar"]["displayText"],
        "uri": response[0]["holidayCalendar"]["uri"]
    }

def get_project_roles_data(response):
    """
    Extract project role data from Replicon list service response.
    
    Expected response structure:
    - header: Array of column definitions  
    - rows: Array of role data with cells containing role info, cost, and billing rate
    
    Returns:
        List of dictionaries with role name, URI, cost, and billing rate information
    """
    if not response:
        return []
        
    return [{
        'name': role['cells'][0].get('textValue'),
        'uri': role['cells'][0].get('uri'),
        'slug': role['cells'][0].get('slug'),
        'cost_amount': role['cells'][1].get('numberValue'),
        'cost_text': role['cells'][1].get('textValue'),
        'cost_currency': role['cells'][1].get('moneyValue', {}).get('baseCurrencyValue', {}).get('currency', {}).get('symbol'),
        'billing_rate': role['cells'][2].get('numberValue'),
        'billing_text': role['cells'][2].get('textValue'),
        'billing_currency': role['cells'][2].get('moneyValue', {}).get('baseCurrencyValue', {}).get('currency', {}).get('symbol')
    } for role in response['rows']]

def get_required_payrules(response, assignment_rules_mapper_data):
    """
    Extracts required pay rules from the response based on the assignment rules mapper data.
    Returns name:uri format.
    """
    # Get all unique pay rules from mapper
    unique_payrules = set()
    for rule in assignment_rules_mapper_data:
        payrule_name = rule.get('payrule')
        if payrule_name:
            unique_payrules.add(payrule_name)
    
    # Get only those pay rules from response in name:uri format
    payrules = {}
    for payrule in response:
        if payrule['displayText'] in unique_payrules:
            payrules[payrule['displayText']] = payrule['uri']
    
    return payrules

def get_required_timesheet_periods(response, assignment_rules_mapper_data):
    """
    Extracts required timesheet periods from the response based on the assignment rules mapper data.
    Returns name:uri format.
    """
    # Get all unique timesheet periods from mapper
    unique_timesheet_periods = set()
    for rule in assignment_rules_mapper_data:
        period_name = rule.get('timesheet_period')
        if period_name:
            unique_timesheet_periods.add(period_name)
    
    # Get only those timesheet periods from response in name:uri format
    timesheet_periods = {}
    if response.get('rows'):
        for period in response['rows']:
            period_name = period['cells'][0]['textValue']
            if period_name in unique_timesheet_periods:
                timesheet_periods[period_name] = period['cells'][0]['uri']
    
    return timesheet_periods

def get_all_office_schedules(response, assignment_rules_mapper_data):
    """
    Get all office schedules for Office Schedule type assignments.
    Returns all available office schedules in name:uri format since input schedule field determines which one to use.
    """
    office_schedules = {}
    for schedule in response:
        office_schedules[schedule['displayText']] = schedule['uri']
    
    return office_schedules
