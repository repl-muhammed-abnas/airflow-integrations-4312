import rail
from airflow.exceptions import AirflowException
LOCATION_DELIMITER = " | "

def get_value(data, index, pluck_key):
    return data[index].get(pluck_key)

def get_location_response_filter(response):
    return list(map(lambda location: {
        "name": get_value(location['cells'] , 0, 'textValue'),
        "uri": get_value(location['cells'] , 0, 'uri'),
        "fullpath": rail.smartjoin_by_delim([location['textValue'] for location in get_value(location['cells'] , 1, 'cellCollection')],
                            LOCATION_DELIMITER)
    }, response['rows']))

def get_employeegroup_response_filter(response):

    employee_data = list(map(lambda employee_type: {
            "name": get_value(employee_type['cells'], 0, 'textValue'),
            "uri": get_value(employee_type['cells'], 0, 'uri'),
            "full_path": rail.smartjoin_by_delim([employee_type['textValue'] for employee_type in get_value(employee_type['cells'] , 1, 'cellCollection')],
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
            "full_path": rail.smartjoin_by_delim([_company_code['textValue'] for _company_code in get_value(company_code['cells'] , 1, 'cellCollection')],
                            LOCATION_DELIMITER),
            "parent":  get_value(company_code['cells'] , 1, 'cellCollection')[0]['textValue']

        }, response['rows']))

def get_all_user_custom_fields_data_handler(config, response):
    UDF_FIELDS = config.UDFs.copy()
    res = {}
    rail.set_result(key= "response", val = response)
    # doing in for loop to avoid multiple iter of response while using rail.find_first_by_attr_and_get_attr
    for udf in response:
        if not UDF_FIELDS:
            break
        if udf['displayText'] in UDF_FIELDS:
            res[udf['displayText'].replace(
                ".", "").replace(" ", "_").lower()] = {"name": udf['displayText'], "uri": udf['uri']}
            UDF_FIELDS.remove(udf['displayText'])
    rail.set_result(key = "udfs_not_found", val=UDF_FIELDS)
    return res


def get_starting_balance_script_data_handler(response):
    script_uri = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', 'Starting Balance Set To', 'uri', None)
    if script_uri:
        return script_uri
    raise AirflowException("Script `Starting Balance Set To` is not found")


def get_prevent_balance_overdraw_script_data_handler(response):
    script_uri = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', 'Prevent balance overdraw', 'uri', None)
    if script_uri:
        return script_uri
    raise AirflowException("Script `Prevent balance overdraw` is not found")

def get_all_user_oef_data_handler(config, response):
    OEF_FIELDS = config.UKI_OEFs.copy()
    res = {}
    rail.set_result(key= "oef_response", val = response)
    for oef in response:
        if not OEF_FIELDS:
            break
        if oef['name'] in OEF_FIELDS:
            res[oef['name'].replace(
                ".", "").replace(" ", "_").lower()] = {"name": oef['name'], "uri": oef['uri']}
            OEF_FIELDS.remove(oef['name'])
    return res

def get_dropdown_options_for_employee_representative_status_filter(response):
    res = dict()
    for item in response:
        res[item['displayText'].lower()] = item['uri']
    return res
