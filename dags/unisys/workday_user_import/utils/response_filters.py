"""
Response Filters Utility - Unisys Workday User Import

Processes and filters Replicon API responses.
This module provides data handlers and filters for transforming raw Replicon API
responses into usable data structures. Functions extract relevant fields, flatten
nested structures, and format data for downstream processing.

Key features:
    - Extract custom field URIs by name
    - Flatten paginated list service responses
    - Filter organizational hierarchies (divisions, locations, user types)
    - Map supervisor data to simplified structure
    - Extract permission set information
    - Filter timesheet periods
    - Validate date constraints
    - Extract effective user group memberships

Functions:
    get_udf_uris(response, udfs): Extract custom field URIs by name
    filter_all_co_costcenters_data(response): Filter division/cost center data
    filter_all_location_data(response): Filter location hierarchy data
    filter_all_employeetype_groups_data(response): Filter user type group data
    filter_timesheet_period_list(response): Filter timesheet period data
    get_required_permission(response, config): Extract required permission sets
    get_filtered_user_data(response): Filter user search results
    is_date_in_past(date_dict): Check if date is in the past
    map_supervisor_list_data(response): Map supervisor search results
    is_assign_supervisorpermission(response): Check if supervisor permission needed
    get_group_value(data, key): Extract group value from response
    get_effective_user_groupmembership_filter(response): Filter user group memberships
"""
import pendulum
import itertools
import rail

def get_udf_uris(response, udfs):
    """
    Extract custom field URIs from response by display text.

    Args:
        response (list): List of custom field definitions from Replicon
        udfs (list): List of custom field display text names to find

    Returns:
        dict: Dictionary mapping field names to URIs
            Keys are lowercase field names with underscores and '_uri' suffix
            Example: 'Department ID' becomes 'department_id_uri'

    Example:
        >>> response = [
        ...     {'displayText': 'Department ID', 'uri': 'urn:replicon:custom-field:dept-id'},
        ...     {'displayText': 'Pay Group', 'uri': 'urn:replicon:custom-field:pay-group'}
        ... ]
        >>> udfs = ['Department ID', 'Pay Group']
        >>> get_udf_uris(response, udfs)
        {
            'department_id_uri': 'urn:replicon:custom-field:dept-id',
            'pay_group_uri': 'urn:replicon:custom-field:pay-group'
        }
    """
    total_udfs = len(udfs)
    udf_uris = {}
    for rec in response:
        if rec['displayText'] in udfs:
            uri = f"{rec['displayText'].lower().replace(' ', '_')}_uri"
            udf_uris[uri] = rec['uri']
            total_udfs -= 1
        if total_udfs == 0:
            break
    return udf_uris

def filter_all_co_costcenters_data(response):
    """
    Filter and format division/cost center data from paginated response.

    Processes paginated list service response for divisions, flattens the data,
    and filters to include only enabled divisions with formatted attributes.

    Args:
        response (list): List of page responses from DivisionListService1.svc/GetData
            Each page contains 'rows' with division data

    Returns:
        list or None: List of division dictionaries containing:
            - name (str): Division display name
            - uri (str): Division URI
            - fullpath (str): Pipe-separated hierarchy path
            - isenabled (str): Enabled status ('True'/'False')
            - code (str): Division code
            - length (int): Hierarchy depth
        Returns None if no enabled divisions found

    Note:
        Only includes divisions where isenabled is True.
    """
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    costcenter_info = list(filter(lambda item: item['isenabled'] in ['True', True], map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        'fullpath': '|'.join(list(map(lambda c: c['textValue'], row['cells'][1]['cellCollection']))),
        'isenabled': row['cells'][2]['textValue'],
        'code': row['cells'][3]['textValue'] if row['cells'][3]["dataType"] == "urn:replicon:list-type:string" else "",
        'length': len(row['cells'][1]['cellCollection']),
    }, flaten_rows)))
    return costcenter_info if costcenter_info else None

def filter_all_location_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    location_info = list(map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        'fullpath': '|'.join(list(map(lambda c: c['textValue'], row['cells'][1]['cellCollection']))),
        'length': len(row['cells'][1]['cellCollection']),
    }, flaten_rows))
    return location_info if location_info else None

def filter_all_employeetype_groups_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    employetype_info = list(map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        'fullpath': '|'.join(list(map(lambda c: c['textValue'], row['cells'][1]['cellCollection']))),
        'length': len(row['cells'][1]['cellCollection']),
    }, flaten_rows))
    return employetype_info if employetype_info else None

def filter_timesheet_period_list(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    ts_period_info = list(map(lambda row: {
        "uri": row["cells"][0]["uri"],
        "name": row["cells"][1].get('textValue')
    }, flaten_rows))
    return ts_period_info if ts_period_info else None

def get_required_permission(response, config):
    return response
    # resp = {
    #     'project_manager_permissionuri': '',
    #     'end_user_with_report_edit_permissionuri': '',
    #     'supervisor_permissionuri': ''
    # }
    # no_of_permissions = len(config.PERMISSIONS)
    # for rec in response:
    #     if rec['displayText'] in config.PERMISSIONS:
    #         per_name = f"{rec['displayText'].replace(' ', '_').lower()}_permissionuri"
    #         resp[per_name] = rec['uri']
    #         no_of_permissions -= 1
    #     if no_of_permissions == 0:
    #         break
    # return resp


def get_filtered_user_data(response):
    return [] if response == [None] else response

def is_date_in_past(date_dict):
    """
    Check if a date is in the past using pendulum.
    
    Args:
        date_dict: Dictionary with 'day', 'month', 'year' keys
        
    Returns:
        bool: True if date is in the past, False otherwise
    """
    date = pendulum.date(date_dict['year'], date_dict['month'], date_dict['day'])
    return date < pendulum.today().date()

def map_supervisor_list_data(response):
    """
    Map supervisor search results to simplified structure.

    Extracts key supervisor attributes from BulkGetUsers3 response and
    determines if supervisor's end date is in the past.

    Args:
        response (list): BulkGetUsers3 response containing user data
            Expected to have at least one user record

    Returns:
        dict or None: Supervisor data dictionary containing:
            - name (str): Supervisor's display name
            - loginname (str): Supervisor's login name
            - uri (str): Supervisor's user URI
            - status (bool): Supervisor's enabled status
            - is_enddate_in_past (bool): True if employment end date is in past
        Returns None if response is empty

    Example:
        >>> response = [{
        ...     'userDetails': {
        ...         'displayText': 'John Doe',
        ...         'uri': 'urn:replicon:user:12345',
        ...         'isEnabled': True,
        ...         'employmentDateRange': {'endDate': {'year': 2024, 'month': 12, 'day': 31}}
        ...     },
        ...     'securityConfiguration': {'loginName': 'jdoe'}
        ... }]
        >>> map_supervisor_list_data(response)
        {
            'name': 'John Doe',
            'loginname': 'jdoe',
            'uri': 'urn:replicon:user:12345',
            'status': True,
            'is_enddate_in_past': True
        }
    """
    if not response:
        return None
    is_enddate_in_past = False
    enddate = response[0]['userDetails']['employmentDateRange']['endDate'] if response[0]['userDetails']['employmentDateRange'] else None
    if enddate and is_date_in_past(enddate):
        is_enddate_in_past = True
    return {
        'name': response[0]['userDetails']['displayText'],
        'loginname': response[0]['securityConfiguration']['loginName'],
        'uri':  response[0]['userDetails']['uri'],
        'status':  response[0]['userDetails']['isEnabled'],
        'is_enddate_in_past': is_enddate_in_past
    }

def is_assign_supervisorpermission(response):
    supervisor_permission = False
    if response:
        if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {}) if data[0].get(key, {}) else {}

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType', 'division', 'costCenter', 'serviceCenter']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

def filter_project_uris(response):
    project_uris = {}
    if response:
        for item in response:
            project_uris[item['code']] = item['uri']
    return [project_uris] if project_uris else None

def filter_project_tasks_hash(response):
    uri_map = {}
    if response:
        for item in response:
            if isinstance(item, dict):
                if item['project']['code'] not in uri_map:
                    uri_map[item['project']['code']] = {
                        "project": item['project']['uri'],
                        "tasks": [item['uri']]
                    }
                    continue
                uri_map[item['project']['code']]['tasks'].append(item['uri'])
    return uri_map
