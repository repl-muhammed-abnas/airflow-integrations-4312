import pendulum
import itertools
import rail

SEPERATOR = ','

def get_udf_uris(response, udfs):
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


def filter_all_division_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    costcenter_info = list(filter(lambda item: item['isenabled'] in ['True', True], map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        'fullpath': SEPERATOR.join(list(map(lambda c: c['textValue'], row['cells'][1]['cellCollection']))),
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
        'fullpath': SEPERATOR.join(list(map(lambda c: c['textValue'], row['cells'][1]['cellCollection']))).lower(),
        'length': len(row['cells'][1]['cellCollection']),
    }, flaten_rows))
    return location_info if location_info else None


def filter_all_employeetype_groups_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    employetype_info = list(map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        'fullpath': SEPERATOR.join(list(map(lambda c: c['textValue'], row['cells'][1]['cellCollection']))),
        'length': len(row['cells'][1]['cellCollection']),
    }, flaten_rows))
    return employetype_info if employetype_info else None


def filter_all_costcenters_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    costcenter_info = list(filter(lambda item: item['isenabled'] in ['True', True], map(lambda row: {
        'name': row['cells'][0]['textValue'],
        'isenabled': row['cells'][1]['textValue'],
        'code': row['cells'][2].get('textValue', ''),
        'uri': row['cells'][0]['uri'],
    }, flaten_rows)))
    return costcenter_info if costcenter_info else None

def filter_all_servicecenters_data(response):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    if not response:
        return []
    servicecenter_info = list(filter(lambda item: item['isenabled'] in ['True', True], map(lambda row: {
        'name': row['cells'][0].get('textValue'),
        'uri': row['cells'][0].get('uri'),
        'fullpath': rail.smartjoin_by_delim([item['textValue'] for item in row['cells'][1]['cellCollection']], '/'),
        'fullpath_code': rail.smartjoin_by_delim([item['textValue'] for item in row['cells'][2]['cellCollection']], '/'),
        'length': len([item['textValue'] for item in row['cells'][1]['cellCollection']]),
        'isenabled': row['cells'][3]['textValue'],
        'code': row['cells'][4].get('textValue', ''),
    }, flaten_rows)))
    return servicecenter_info if servicecenter_info else None

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


def get_filtered_user_data(response):
    return [] if response == [None] else response


def is_date_in_past(date_dict):
    date = pendulum.date(date_dict['year'], date_dict['month'], date_dict['day'])
    return date < pendulum.today().date()


def map_supervisor_list_data(response):
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
