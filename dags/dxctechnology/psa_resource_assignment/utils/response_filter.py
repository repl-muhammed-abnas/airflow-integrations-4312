from dxctechnology.psa_resource_assignment.utils import custom_methods
null = None


def map_resource_assignment_list(response, dag_run):
    data = response.json()['d']
    user = dag_run.conf['useruri']
    return list(filter(lambda x: x['status'] == "Yes", list(map(lambda item: {
        "uri": item['resource']['uri'],
        "startdate": item['projectAssignmentDateRange']['startDate'],
        "enddate": item['projectAssignmentDateRange']['endDate'],
        "status": "Yes" if user == item['resource']['uri'] else "No",
    }, data))))


def map_parent_column_uri(response):
    data = response.json()['d']
    basic_uris = list(filter(lambda x: x['displayText'] == "Basic", data))
    return list(filter(lambda x: x['displayText'] == "Parent WBS", basic_uris[0]['columns']))


def map_parent_wbs_oef_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['name'] == "Parent WBS", data))


def map_child_wbs(response):
    data = response.json()['d']['rows']
    return list(map(lambda item: item['cells'][0]['textValue'], list(filter(lambda x: x['cells'][1]['textValue']
                                                                            == custom_methods.get_conf()['wbs'], data))))
