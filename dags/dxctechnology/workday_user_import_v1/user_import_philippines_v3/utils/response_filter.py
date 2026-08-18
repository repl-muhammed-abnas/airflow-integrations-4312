from rail import (set_result, smartjoin_by_delim, find_first_by_attr_and_get_attr)
from airflow.exceptions import AirflowException

from dxctechnology.workday_user_import_v1.user_import_philippines_v3.utils.request_payload import LOCATION_DELIMITER

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
