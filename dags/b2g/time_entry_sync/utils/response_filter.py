
def get_filtered_user_data(response,dag_run):
    data = response.json()['d']
    return list(filter(lambda x: bool(x['name']) and x['name'] == dag_run.conf['User_Name'], map(lambda row: {
        "name": row['cells'][0]['textValue'] if row['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
        "uri": row['cells'][0]['uri']
    }, data['rows'])))

def check_task_data(response,dag_run):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['Taskcode'] == dag_run.conf['Task_Code'], list(map(lambda item: {
        "Taskname": item['cells'][0]['textValue'],
        "Taskstatus": item['cells'][1]['textValue'],
        "Taskuri": item['cells'][0]['uri'],
        "Taskcode": item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
    }, response['rows']))))
