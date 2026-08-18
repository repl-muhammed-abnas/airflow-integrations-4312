import ast


def get_all_assignee_ids(response):
    data = response['tags']
    result = list(map(lambda item: {
        'assigneeid': item['name'],
        'assigneename': item['code'],
        'assigneeuri': item['uri'],
        'status': item['isEnabled']
    }, data))
    return ast.literal_eval(str(result))


def get_all_projects_filtered(response, dag_run):
    data = response['rows']
    result= list(filter(lambda x: (x['clientname'] == dag_run.conf['clientname']) and (x['clienturi'] == dag_run.conf['clienturi']), map(lambda item: {
        'projectname': item['cells'][0]['textValue'],
        'projecturi': item['cells'][0]['uri'],
        'clientname': item['cells'][1]['textValue'] if item['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        'clienturi': item['cells'][1]['uri'] if item['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None
    }, data)))
    if result:
        return list(map(lambda i: {
            "row": i, **result[i]
        }, range(len(result))))

    return []

def get_filtered_assignee_details(response, dag_run):
    data = response['rows']
    if not data:
        return []
    assigneedata = list(filter(lambda x: x['assigneeid']== dag_run.conf['assignee_id'], map(lambda item: {
        'assigneeid': item['cells'][0]['textValue'],
        'assigneename': item['cells'][1]['textValue'] if item['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        'assigneeuri': item['cells'][3]['uri'],
        'status': item['cells'][4]['boolValue']
    },data)))
    return assigneedata[0] if assigneedata else []
