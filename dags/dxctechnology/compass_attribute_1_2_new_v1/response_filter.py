from dxctechnology.compass_attribute_1_2_new_v1.request_payload import get_conf
null = None


def map_attribute_1_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "Attribute 1", data))


def map_attribute_2_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "Attribute 2", data))


def map_get_specific_attribute_system_level(response):
    data = response.json()['d']['rows']
    if data:
        return list(filter(lambda x: x['cells'][0]['textValue'] == get_conf()['attribute_name'].strip(), data))
    return []


def map_get_project_details(response):
    data = response.json()['d']
    if not data[0]['error']:
        return list(map(lambda project: {
            "uri": project['projectDetails']['uri'],
            "status": project['projectDetails']['status']['name'],
            "start_date_year": project['projectDetails']['timeEntryDateRange']['startDate']['year']
            if project['projectDetails']['timeEntryDateRange']['startDate'] else null,
            "start_date_month": project['projectDetails']['timeEntryDateRange']['startDate']['month']
            if project['projectDetails']['timeEntryDateRange']['startDate'] else null,
            "start_date_day": project['projectDetails']['timeEntryDateRange']['startDate']['day']
            if project['projectDetails']['timeEntryDateRange']['startDate'] else null,
            "end_date_year": project['projectDetails']['timeEntryDateRange']['endDate']['year']
            if project['projectDetails']['timeEntryDateRange']['endDate'] else null,
            "end_date_month": project['projectDetails']['timeEntryDateRange']['endDate']['month']
            if project['projectDetails']['timeEntryDateRange']['endDate'] else null,
            "end_date_day": project['projectDetails']['timeEntryDateRange']['endDate']['day']
            if project['projectDetails']['timeEntryDateRange']['endDate'] else null,
            "extensionFieldValue": project['projectDetails']['extensionFieldValues']
        }, data))
    return []


def map_get_specific_attribute_project_level(response):
    data = response.json()['d']
    attribute = get_conf()['attribute_value'] + \
        " - " + get_conf()['Description']
    return list(filter(lambda atr: atr['tag']['displayText'] == attribute, data))


def map_get_attribute_system_level_project(response):
    data = response.json()['d']['rows']
    return list(filter(lambda x: x['cells'][0]['textValue'] == (get_conf()['attribute_value'] + " - " + get_conf()['Description']).strip(), data))


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
                                                                            == get_conf()['wbs'], data))))
