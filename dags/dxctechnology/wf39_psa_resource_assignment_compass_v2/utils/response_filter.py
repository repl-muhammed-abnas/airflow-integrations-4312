import itertools
from dxctechnology.wf39_psa_resource_assignment_compass_v2.utils import request_payload


def map_billing_rates(response):
    data = response.json()['d']
    return list(map(lambda item: {
        "displayText": item['displayText'],
        "name": item['name'],
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
