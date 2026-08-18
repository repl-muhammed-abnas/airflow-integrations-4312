
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

def completed_exports_list(response):
    if not (response and response["rows"]):
        return []

    return list(filter(lambda x: x['status'] == 'Complete', map(lambda item: {
        'timeexport': item["cells"][0]['textValue'],
        'status': item["cells"][1]['textValue'],
        'creationdate': item["cells"][2]['textValue'],
        'uri': item["cells"][0]['uri']
    }, response["rows"])))

def past_14days_time_exports_for_C1(response):
    if not response:
        return []

    return list(map(lambda item: {
        'twbname': item["cells"][0]['textValue'],
        'status': item["cells"][1]['textValue'],
        'creationdate': item["cells"][2]['textValue'],
        'twburi': item["cells"][0]['uri']
    }, response["rows"]))

def get_unacknowledged_time_export_details(dag_run):
    response = rail.result("get_specific_time_export_details")['extensionFieldValues']

    if not response:
        return True

    return rail.find_first_by_attr_and_get_attr(response, "definition.displayText", dag_run.conf["oef_name"], "textValue", "") != "Yes"

def filter_employee_groups(response):
    return {
        "contractor_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contractor", "uri"),
        "agency_contractor_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Agency Contractor", "uri")
    }
