from macquariegroup.user_import.mapper.recovery_field_mapper import recovery_field_mapper
from macquariegroup.user_import.utils.custom_methods import get_str_date

DEPARTMENT_DELIMITER = "^"
null_urn = "urn:replicon:list-type:null"


def get_value(item, index, pluck_key):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""


def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return DEPARTMENT_DELIMITER.join([item['textValue'] for item in full_path_list])


def get_all_department_filter(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data['cells'], 0, 'textValue'),
        "uri": get_value(data['cells'], 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))


def get_all_cost_centers_filter(response):
    if not response:
        return []
    return list(map(lambda item: {
        "name": item['displayText'],
        "uri": item['uri']
    }, response))


def get_all_timesheet_period_filter(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data['cells'], 1, 'textValue'),
        "enabled": get_value(data['cells'], 0, 'textValue'),
        "uri": get_value(data['cells'], 1, 'textValue')
    }, response['rows']))


def get_all_user_custom_fields_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))

def get_all_user_oef_fields_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['name'],
        "uri": data['uri'],
    }, response))

def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))


null = None


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
        effective_groups["department"] = {
            "name": response['departments'][0]['department']['department']['displayText'],
            "uri": response['departments'][0]['department']['department']['uri']
        }

    if response['divisions']:
        effective_groups["division"] = {
            "name": response['divisions'][0]['division']['division']['displayText'],
            "uri": response['divisions'][0]['division']['division']['uri']
        }

    if response['employeeTypes']:
        effective_groups['employee_type'] = {
            "name": response['employeeTypes'][0]['employeeType']['employeeType']['displayText'],
            "uri": response['employeeTypes'][0]['employeeType']['employeeType']['uri']
        }

    if response['serviceCenters'] and response['serviceCenters'][0]['serviceCenter']:
        effective_groups['service_center'] = {
            "name": response['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'],
            "uri": response['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri']
        }

    return effective_groups


def get_timesheet_uris(response):
    data = list(map(lambda item: {
        'uri': item['cells'][0]['uri']
    }, response['rows']))
    return [x['uri'] for x in data if x['uri']]


def get_timesheet_details_payload(dag_run):
    effective_date = list(filter(
        lambda item: item['employee_type'] == dag_run.conf['employee_type'], recovery_field_mapper))
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:due-date"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:due-date"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": effective_date[0]['timesheet_period_assignment']
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['user_uri']
                    }
                }
            }
        }
    }


def get_holiday_date_list(response):

    if not response:
        return []

    res = []
    for date in response:
        res.append(get_str_date(date['date'], is_dict=True))

    return res
