from dxctechnology.c1_iwo_leanstaffing import request_payload
null = None


def map_billing_rates(response):
    data = response.json()['d']
    return list(map(lambda item: {
        "displayText": item['displayText'],
        "name": item['name'].replace("|Billable", "").replace("|Non-Billable", "").strip(),
        "uri": item['uri']
    }, data))


def map_resource_assignment_list(response):
    data = response.json()['d']
    conf = request_payload.get_dag_run_conf()
    user = conf['useruri']
    return list(filter(lambda x: x['status'] == "Yes", list(map(lambda item: {
        "uri": item['resource']['uri'],
        "startdate": item['projectAssignmentDateRange']['startDate'],
        "enddate": item['projectAssignmentDateRange']['endDate'],
        "status": "Yes" if user == item['resource']['uri'] else "No",
        "billingRatesAllowedForBillingTime": item['billingRatesAllowedForBillingTime']
    }, data))))


def map_parent_wbs_oef_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['name'] == "Parent WBS", data))


def map_parent_column_uri(response):
    data = response.json()['d']
    basic_uris = list(filter(lambda x: x['displayText'] == "Basic", data))
    return list(filter(lambda x: x['displayText'] == "Parent WBS", basic_uris[0]['columns']))


def map_child_wbs(response):
    data = response.json()['d']['rows']
    return list(map(lambda item: item['cells'][0]['textValue'], list(filter(lambda x: x['cells'][1]['textValue']
                                                                            == request_payload.get_dag_run_conf()['wbselement'], data))))
