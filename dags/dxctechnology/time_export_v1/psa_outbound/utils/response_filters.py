import rail

null = None

def get_specific_time_export_details(timedata, oefname):
    if not timedata:
        return True
    return rail.find_first_by_attr_and_get_attr(timedata, "definition.displayText", oefname, "textValue", "") != "Yes"

def get_psa_org_unit_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, "displayText", "PSA Org Unit", "uri")

def get_psa_child_hierarchy_list(response):
    return list(map(lambda row: row["cells"][0]["textValue"], response["rows"]))

def get_psa_cost_center_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, "displayText", "PSA Cost Center", "uri")
