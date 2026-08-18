"""
ViaPlus User Sync - Response Filter Utilities

Functions to filter and transform Replicon API responses.
"""
import rail

GROUPS_DELIMITER = '|'

def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])

def filter_full_path_data(response):
    """Filter and flatten location/division data with full path."""
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data, 0, 'textValue'),
        "uri": get_value(data, 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))


def filter_group_data(response):
    """Filter service center (legal entity) data."""
    if not response['rows']:
        return []
    return list(map(lambda item: {
        "name": get_value(item, 0, "textValue"),
        "code": get_value(item, 1, "textValue"),
        "uri": get_value(item, 2, "uri")
    }, response['rows']))


def filter_employee_type_data(response):
    """Filter employee type group data."""
    return list(
        map(lambda data:
            {
                'name': get_value(data, 0, 'textValue'),
                'uri': get_value(data, 0, 'uri')
            }, response['rows'])
    )


def get_filtered_time_off_types(response):
    """Filter time off types to get required fields."""
    if not response:
        return []

    return [
        {
            'timeoff_type_name': item.get('displayText', ''),
            'timeoff_type_uri': item.get('uri', ''),
            'enabled': item.get('isEnabled', True)
        }
        for item in response
    ]


def filter_licenses(response, config):
    license_uris = []
    for item in response:
        if item.get('displayText') in config.LICENSES_TO_ASSIGN:
            license_uris.append({
                "uri": item.get('uri')
            })
    return license_uris


def get_missing_permissions(response, dag_run):
    """
    Check if supervisor has required permissions and return missing ones.
    Matches CRL user_import_ireland_v1 pattern.
    """
    permissions_to_add = []

    # Check for Supervisor permission
    if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision'):
        permissions_to_add.append(dag_run.conf['supervisor_permission_uri'])

    # Check for Report User permission
    if rail.find_first_by_attr_and_get_attr(
        response, 'policyUri', 'urn:replicon:policy:user', 'permissionSet.displayText'
    ) != "Project Resource with Reports":
        if dag_run.conf.get('report_user_permission_uri'):
            permissions_to_add.append(dag_run.conf['report_user_permission_uri'])

    return permissions_to_add

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'employeeType', 'costCenter','department']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))