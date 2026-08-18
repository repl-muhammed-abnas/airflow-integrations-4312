import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


null_urn = "urn:replicon:list-type:null"
def project_list(response):
    response = response.json()['d']['rows']
    if not response:
        return []

    return list(map(lambda item: {
        "Projectname": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != null_urn else None,
        "Projecturi": item['cells'][0]['uri'] if item['cells'][0]['dataType'] != null_urn else None,
        "Projectstatus": item['cells'][1]['textValue'] if item['cells'][1]['dataType'] != null_urn else None,
        "Projectstartdate": item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != null_urn else None,
        "Projectenddate": item['cells'][3]['textValue'] if item['cells'][3]['dataType'] != null_urn else None
    }, response))


def get_task_custom_field(response):
    response = response.json()['d']

    Lasttimeentrydate_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Lasttimeentrydate', 'uri', default="")
    createdby_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'createdby', 'uri', default="")
    issuetype_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Issue Type', 'uri', default="")
    parentid_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Parent ID', 'uri', default="")
    epiclink_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Epic Link', 'uri', default="")
    epicid_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Epic ID', 'uri', default="")
    epicsummary_uri = rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Epic Summary', 'uri', default="")
    return Lasttimeentrydate_uri, createdby_uri, issuetype_uri, parentid_uri, epiclink_uri, epicid_uri, epicsummary_uri


def get_project_custom_field(response):
    response = response.json()['d']

    return rail.find_first_by_attr_and_get_attr(
        response, 'displayText', 'createdby', 'uri', default="")


def client_uri_check(response):
    dag_run_conf = get_dag_run_conf()
    response = response.json()['d']['rows']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == dag_run_conf['customer'],list(map(lambda item: {
        "name": item['cells'][0]['textValue'],
        "uri": item['cells'][0]['uri'],
    }, response))))


def put_client(response):
    response = response.json()['d']

    return response['uri']


def check_task_data(response):
    dag_run_conf = get_dag_run_conf()
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['Taskname'] == dag_run_conf['Key'], list(map(lambda item: {
        "Taskname": item['cells'][0]['textValue'],
        "Taskstatus": item['cells'][1]['textValue'],
        "Taskuri": item['cells'][0]['uri'],
    }, response['rows']))))


def get_all_team_members_data(response):
    response = response.json()['d']
    if not response:
        return []

    resource_uris=list(map(lambda item: {
        "uri": item['resource']['uri']
    }, response))

    return [x['uri'] for x in resource_uris]
