import rail


def get_missing_wbs_length():
    missing_wbs = rail.result('get_records_missing_wbs_from_xml')
    return len(missing_wbs)


def get_iwo_wbs_element(project_data):
    jsonValue = rail.result(project_data)[0]['extensionFieldValue']
    return list(filter(lambda x: x['definition']['displayText'] == "IWO WBS Element", jsonValue))
