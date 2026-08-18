import json
from airflow.exceptions import AirflowException
import rail

null = None

def filter_users_data_from_source(response, api_keys_mapper):
    # Assuming 'response.text' contains your JSON data
    data = json.loads(response.text)
    
    # Access result entries
    entries = data.get('resultEntries', [])
    
    # Return empty list if no entries
    if not entries:
        return []

    # Helper function to get first value from array or return None
    def get_first_value(attributes, attr_name):
        value = attributes.get(attr_name)
        if value and isinstance(value, list) and len(value) > 0:
            return value[0]
        return null

    # Process each user entry
    users_data = []
    for entry in entries:
        attributes = entry.get('attributes', {})
        
        # Filter and transform data using mapper for this user
        filtered_user_data = {}
        for output_field, api_field in api_keys_mapper.items():
            filtered_user_data[output_field] = get_first_value(attributes, api_field)
        
        users_data.append(filtered_user_data)

    return rail.write_json_artifact(users_data)

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

def get_all_holiday_calendars(response):
    return [{
        "holiday_calendar_name": holiday_calendar["displayText"],
        "uri": holiday_calendar["uri"]
    } for holiday_calendar in response]

def get_all_oef_tags(response):
    return [{
        "oef_tag": oef_data["name"],
        "code": oef_data["code"],
        "description": oef_data["description"],
        "is_enabled": oef_data["isEnabled"],
        "uri": oef_data["uri"]
    } for oef_data in response["tags"]]

def get_required_timeoffs_data(response, timeoff_types_mapper_data):
    """
    Extracts required time off types from the response based on the provided mapper data.
    """
    # Get all unique time off types from mapper
    unique_timeoff_types = set()
    for mapper in timeoff_types_mapper_data:
        unique_timeoff_types.update(mapper['time_off_types'])
    
    # Get only those time off types from response
    time_off_types = {}
    for timeoff_type in response:
        if timeoff_type['displayText'] in unique_timeoff_types:
            time_off_types[timeoff_type['displayText']] = timeoff_type['uri']
    
    return time_off_types

def get_required_permissions_data(response, permissions_mapper_data):
    """
    Extracts required permissions from the response based on the provided mapper data.
    """
    # Get all unique permissions from mapper
    unique_permissions = set()
    for mapper in permissions_mapper_data:
        unique_permissions.update(mapper['permission'])
    
    # Get only those permissions from response
    permission_set = {}
    for permission in response:
        if permission['displayText'] in unique_permissions:
            permission_set[permission['displayText']] = permission['uri']
    
    return permission_set

def get_required_timesheet_templates_data(response, timesheet_template_mapper_data):
    """
    Extracts required timesheet templates from the response based on the provided mapper data.
    """
    # Get all unique timesheet templates from mapper
    unique_timesheet_templates = set()
    
    # Combine standard and exceptions into one list for processing
    all_mappings = []
    all_mappings.extend(timesheet_template_mapper_data.get("standard", []))
    all_mappings.extend(timesheet_template_mapper_data.get("exceptions", []))
    
    # Process all mappings at once
    for mapper in all_mappings:
        template_name = mapper.get('timesheet_template')
        if template_name:  # Skip empty templates
            unique_timesheet_templates.add(template_name)
    
    # Get only those timesheet templates from response
    timesheet_templates = {}
    for template in response:
        if template['displayText'] in unique_timesheet_templates:
            timesheet_templates[template['displayText']] = template['uri']
    
    return timesheet_templates

def get_required_activities(response, activities_mapper_data):
    unique_activities = set()
    for mapper in activities_mapper_data:
        unique_activities.update(mapper["activities"])
    activities = {}
    for activity in response:
        if activity["displayText"] in unique_activities:
            activities[activity["displayText"]] = activity["uri"]
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
        'code': group['cells'][3].get('textValue', ''),
    } for group in response['rows']]

def get_user_current_holiday_calendar(response):
    if not response:
        return {}
    return {
        "holiday_calendar_name": response[0]["holidayCalendar"]["displayText"],
        "uri": response[0]["holidayCalendar"]["uri"]
    }
