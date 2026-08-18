
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

def get_timeexport_fileformat(response, time_export_file_format):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{time_export_file_format}`')

def completed_exports_list(response):
    if not (response and response["rows"]):
        return []

    return list(filter(lambda x: x['status'] == 'Complete', map(lambda item: {
        'timeexport': item["cells"][0]['textValue'],
        'status': item["cells"][1]['textValue'],
        'creationdate': item["cells"][2]['textValue'],
        'uri': item["cells"][0]['uri']
    }, response["rows"])))

def get_specific_time_export_details(timedata, oefname):
    if not timedata:
        return True
    return rail.find_first_by_attr_and_get_attr(timedata, "definition.displayText", oefname, "textValue", "") != "Yes"

def filter_employee_groups(response):
    return {
        "contractor_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contractor", "uri"),
        "agency_contractor_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Agency Contractor", "uri")
    }
