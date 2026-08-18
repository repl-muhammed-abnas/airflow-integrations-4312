null = None


def map_all_divisions(response):
    data = response.json()['d']['rows']
    c1_codes = list(map(lambda x: x['cells'][0]['textValue'], list(filter(
        lambda item: item['cells'][1]['cellCollection'][0]['textValue'] == "C1", data))))
    compass_codes = list(map(lambda x: x['cells'][0]['textValue'], list(filter(
        lambda item: item['cells'][1]['cellCollection'][0]['textValue'] == "COMPASS", data))))
    GSAP_codes = list(map(lambda x: x['cells'][0]['textValue'], list(filter(
        lambda item: item['cells'][1]['cellCollection'][0]['textValue'] == "GSAP", data))))
    return {"C1": c1_codes, "Compass": compass_codes, "Gsap": GSAP_codes}


def map_emp_details(response, dag_run):
    data = response.json()['d']['rows']
    result = list(
        filter(lambda x: x['cells'][1]['textValue'] == dag_run.conf['empid'], data))
    return result


def map_resource_assignment_list(response, dag_run):
    data = response.json()['d']
    user = dag_run.conf['useruri']
    return list(filter(lambda x: x['status'] == "Yes", list(map(lambda item: {
        "uri": item['resource']['uri'],
        "startdate": item['projectAssignmentDateRange']['startDate'],
        "enddate": item['projectAssignmentDateRange']['endDate'],
        "status": "Yes" if user == item['resource']['uri'] else "No",
    }, data))))
