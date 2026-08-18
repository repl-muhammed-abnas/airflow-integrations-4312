import rail
from mammoet.user_import_v1.utils.custom_methods import LOCATION_DELIMITER

def get_full_path(path_list):
    if not path_list:
        return ""
    return rail.smartjoin_by_delim(([item.get('textValue') for item in path_list]), LOCATION_DELIMITER)

def get_value(data, index, pluck_key):
    return data[index].get(pluck_key)

def get_groups_data_handler(response):
    if not response['rows']:
        return []
    return list(map(lambda record: {
        "name": get_value(record['cells'], 0, "textValue"),
        "uri": get_value(record['cells'], 0, "uri"),
        "enabled": get_value(record['cells'], 1, "textValue"),
        "full_path": get_full_path(get_value(record['cells'], 2, "cellCollection")),
        "code": get_value(record['cells'], 3, "textValue")
    }, response['rows']))

def get_effectivegroup_membership_filter(response):
    if not response:
        return []
    effective_groups = {}
    if response['costCenters']:
        effective_groups["cost_center"] = {
            "name": response['costCenters'][0]['costCenter']['costCenter']['displayText'],
            "uri": response['costCenters'][0]['costCenter']['costCenter']['uri']
        }

    if response['departments']:
        effective_groups["legal_entities"] = {
            "name": response['departments'][0]['department']['department']['displayText'],
            "uri": response['departments'][0]['department']['department']['uri']
        }

    if response['employeeTypes']:
        effective_groups['employee_type'] = {
            "name": response['employeeTypes'][0]['employeeType']['employeeType']['displayText'],
            "uri": response['employeeTypes'][0]['employeeType']['employeeType']['uri']
        }

    if response['serviceCenters'] and response['serviceCenters'][0]['serviceCenter']:
        effective_groups['pay_grade'] = {
            "name": response['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'],
            "uri": response['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri']
        }

    if response['locations'] and response['locations'][0]['location']:
        effective_groups['location'] = {
            "name": response['locations'][0]['location']['location']['displayText'],
            "uri": response['locations'][0]['location']['location']['uri']
        }

    return effective_groups


def get_required_activities(response):
    return {
        "activities": response
    }

def get_all_holiday_calenders_data_handler(response):
    return {
            holiday['displayText']: holiday for holiday in response
        }
