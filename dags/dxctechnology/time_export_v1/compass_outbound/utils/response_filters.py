
import itertools
import rail
null = None

def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def filter_divisions_data(result):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))
    return list(map(lambda row: {
        "name": get_value(row, 0, 'textValue'),
        "code": get_value(row, 1, 'textValue'),
        "uri": get_value(row, 0, 'uri')
    }, flaten_rows))

def get_cellcollection(data):
    return list(map(lambda cells: cells["textValue"], get_value(data, 1, 'cellCollection')))

def filter_effectively_enabled_compass_division_data(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "companycodename": get_value(data, 0, 'textValue'),
        "fullpath": "/".join(get_cellcollection(data)),
        "parent": get_cellcollection(data)[-2] if len(get_cellcollection(data)) > 1 else null,
        "companycode": get_value(data, 2, 'textValue'),
        "status": get_value(data, 3, 'textValue'),
        "description": get_value(data, 4, 'textValue')
    }, response['rows']))

def completed_exports_list(response):
    if not (response and response["rows"]):
        return []

    return list(filter(lambda x: x['status'] == 'Complete', map(lambda item: {
        'timeexport': item["cells"][0]['textValue'],
        'status': item["cells"][1]['textValue'],
        'creationdate': item["cells"][2]['textValue'],
        'uri': item["cells"][0]['uri']
    }, response["rows"])))

def get_specific_time_export_details(time_export_details, oefvalue):
    if not time_export_details:
        return True
    return rail.find_first_by_attr_and_get_attr(time_export_details, "definition.displayText", oefvalue, "textValue", "") != "Yes"

def filter_employee_groups(response):
    return {
        "contractor_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contractor", "uri"),
        "agency_contractor_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Agency Contractor", "uri")
    }

def filter_divisions_with_description(response):
    if not response['rows']:
        return []
    
    return list(map(lambda data: {
        "companycodename": get_value(data, 0, 'textValue'),
        "description": get_value(data, 1, 'textValue')
    }, response['rows']))
