import itertools
from dxctechnology.wf39_psa_resource_assignment_v4.utils import request_payload


def map_billing_rates(response):
    data = response.json()['d']
    return list(map(lambda item: {
        "displayText": item['displayText'],
        "name": item['name'].replace("|Billable", "").replace("|Non-Billable", "").strip(),
        "uri": item['uri']
    }, data))


def map_project_response(response):
    return (response.json()['d'][0:1] or [
            {"projectDetails": None}])[0]['projectDetails']


def map_resource_assignment_list(response):
    data = response.json()['d']
    conf = request_payload.get_dag_run_conf()
    user = conf['useruri']
    user_data = list(filter(lambda x: x['status'] == "Yes", list(map(lambda item: {
        "uri": item['resource']['uri'],
        "startdate": item['projectAssignmentDateRange']['startDate'],
        "enddate": item['projectAssignmentDateRange']['endDate'],
        "status": "Yes" if user == item['resource']['uri'] else "No",
        "billingRatesAllowedForBillingTime": item['billingRatesAllowedForBillingTime']
    }, data))))

    labour_type_data = list(map(lambda item: {
        "labour_type": [x['billingRate']['displayText'].replace("|Billable", "").replace("|Non-Billable", "").strip() for x in item[
            'billingRatesAllowedForBillingTime']] if item['billingRatesAllowedForBillingTime'] else None
    }, data))

    return {
        'user_data': user_data,
        'labour_type_data': list(itertools.chain(*[x['labour_type'] for x in labour_type_data if x['labour_type'] is not None]))
    }

def get_all_labour_types(response):
    labour_types = []
    data = response['results']
    if data:
        if data[0]['timeAndMaterials']['projectBillingRates']:
            labour_types = list(map(lambda item: {
               'name': item['billingRate']['name']
            }, data[0]['timeAndMaterials']['projectBillingRates']))
    return list(itertools.chain(x['name'] for x in labour_types))

def get_assigned_labor_types(response):
    data = response['results'][0]['timeAndMaterials']['projectBillingRates']
    return list(map(lambda item:
        item['billingRate']['displayText']
    ,data
    ))

def map_current_team_assignments(response):
    """
    Map the current team-member assignment state for a project, keyed by user uri.
    Used by the idempotency gate to detect records that are already in sync.
    Returns: { user_uri: { 'startDate': {y,m,d}|None,
                           'endDate': {y,m,d}|None,
                           'labour_types': [normalised lowercase names] } }
    """
    data = response.json()['d']
    current_state = {}
    for item in data:
        resource = item.get('resource') or {}
        user_uri = resource.get('uri')
        if not user_uri:
            continue
        date_range = item.get('projectAssignmentDateRange') or {}
        labour_types = set()
        for billing_rate in (item.get('billingRatesAllowedForBillingTime') or []):
            name = (billing_rate.get('billingRate') or {}).get('displayText')
            if not name:
                continue
            name = name.replace("|Billable", "").replace("|Non-Billable", "").strip().lower()
            if name:
                labour_types.add(name)
        current_state[user_uri] = {
            'startDate': date_range.get('startDate'),
            'endDate': date_range.get('endDate'),
            'labour_types': sorted(labour_types)
        }
    return current_state
