null = None

def get_level_1_locations_payload(parent_location_uri=None):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:code"
        ],
        "parentUri": parent_location_uri
    }


def get_all_cost_centers_payload():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center"
        ],
        "filterExpression": null,
        "hierarchyListDataOptionUris": []
    }


def build_task_resource_payload(item):
    task_name_chain = [task.strip() for task in item["task_name_full_path"].split("/")]
    task_name = task_name_chain[-1]
    parent_task_name_chain = task_name_chain[:-1]
    project_code = item["project_code"]
    login_name = item["login_name"]

    project = {
        "uri": None,
        "name": None,
        "code": project_code,
        "parameterCorrelationId": None,
    }

    # build parent chain from innermost to outermost
    parent = None
    for index, name in enumerate(parent_task_name_chain):
        node = {
            "uri": null,
            "name": name,
            "parent": parent,
            "project": project if index == 0 else None,
            "parameterCorrelationId": None,
        }
        parent = node

    return {
        "task": {
            "uri": None,
            "name": task_name,
            "parent": parent,
            "project": project if not parent_task_name_chain else None,
            "parameterCorrelationId": None,
        },
        "user": {
            "uri": None,
            "loginName": login_name,
            "employeeId": None,
            "parameterCorrelationId": None,
        },
        "parameterCorrelationId": None,
    }