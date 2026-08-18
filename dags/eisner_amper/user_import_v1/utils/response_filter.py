# pylint: disable=too-many-statements


def check_client_data(response, dag_run):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['available'], list(map(lambda item: {
        "name": item['cells'][0]['textValue'],
        "loginname": item['cells'][1]['textValue'],
        "uri": item['cells'][0]['uri'],
        "status": item['cells'][2]['textValue'],
        "available": "Yes" if item['cells'][1]['textValue'] == dag_run.conf['username'] else "No",
    }, response['rows']))))


def get_effectivegroup_membership_filter(response):
    if not response:
        return []
    effective_groups = {}
    if response['costCenters']:
        effective_groups["cost_center"] = {
            "name": response['costCenters'][0]['costCenter']['costCenter']['displayText'],
            "uri": response['costCenters'][0]['costCenter']['costCenter']['uri']
        }

    if response['departments']:
        effective_groups["legal_entities"] = {
            "name": response['departments'][0]['department']['department']['displayText'],
            "uri": response['departments'][0]['department']['department']['uri']
        }

    if response['divisions']:
        effective_groups['divisions'] = {
            "name": response['divisions'][0]['division']['division']['displayText'],
            "uri": response['divisions'][0]['division']['division']['uri']
        }

    if response['employeeTypes']:
        effective_groups['employee_type'] = {
            "name": response['employeeTypes'][0]['employeeType']['employeeType']['displayText'],
            "uri": response['employeeTypes'][0]['employeeType']['employeeType']['uri']
        }

    if response['serviceCenters'] and response['serviceCenters'][0]['serviceCenter']:
        effective_groups['pay_grade'] = {
            "name": response['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'],
            "uri": response['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri']
        }

    if response['locations'] and response['locations'][0]['location']:
        effective_groups['location'] = {
            "name": response['locations'][0]['location']['location']['displayText'],
            "uri": response['locations'][0]['location']['location']['uri']
        }

    return effective_groups
