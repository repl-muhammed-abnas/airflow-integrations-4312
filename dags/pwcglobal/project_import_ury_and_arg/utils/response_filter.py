from rail import result
null = None
object_list_type_uri = 'urn:replicon:list-type:object'
string_list_type_uri = 'urn:replicon:list-type:string'
bool_list_type_uri = 'urn:replicon:list-type:bool'

def get_data_from_list_service(response):
    if not response['rows']:
        return []
    code = result("get_query_data")['clientcode']

    return list(filter(lambda item: item['code'] == code,map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "code": row['cells'][1]['textValue'],
    }, response['rows'])))

def search_projectmanager_response_filter(dag_run, response):
    return list(filter(lambda item: item['projectmanager_employee_id'] == dag_run.conf['projectmanager_partyid'], map(lambda row: {
            "projectmanager_uri": row['cells'][0]['uri'],
            "projectmanager_employee_id": row['cells'][1]['textValue'] if row['cells'][1]['dataType'] == 'urn:replicon:list-type:string' else '',
            "is_enable": row['cells'][2]['textValue'],
            "legal_entity": row['cells'][3]['cellCollection'][0]['textValue'],
        }, response['rows'])))

def search_engagementpartner_response_filter(dag_run, response):
    return list(filter(lambda item: item['engagementpartner_employee_id'] == dag_run.conf['engagementpartner_partyid'], map(lambda row: {
            "engagementpartner_uri": row['cells'][0]['uri'],
            "engagementpartner_employee_id": row['cells'][1]['textValue'] if row['cells'][1]['dataType'] == 'urn:replicon:list-type:string' else '',
            "is_enable": row['cells'][2]['textValue'],
            "legal_entity": row['cells'][3]['cellCollection'][0]['textValue'],
        }, response['rows'])))

def format_project_task_details(response):
    tasks = []

    def add_child_taks(child_tasks, parent):
        for child_task in child_tasks:
            child_task['task']['full_task_name'] = f"{parent}|{child_task['task']['name']}"
            tasks.append(child_task['task'])
            if child_task['childTasks']:
                add_child_taks(child_task['childTasks'], child_task['task']['name'])

    for item in response:
        item['task']['full_task_name'] = f"{item['task']['name']}"
        tasks.append(item['task'])
        add_child_taks(item['childTasks'], item['task']['name'])

    tasks = list(map(lambda item: {
        "task_name": item['name'],
        "task_code": item['code'],
        "allow_time_entry": "Yes" if item['isTimeEntryAllowed'] else "No",
        "uri": item['uri'],
        "full_task_name": item['full_task_name'],
        "enddate": str(item['timeEntryDateRange']['endDate']['month']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['day']) + '/' + 
        str(item['timeEntryDateRange']['endDate']['year']) if item['timeEntryDateRange'] and \
            item['timeEntryDateRange']['endDate'] else null
    }, tasks))

    return tasks
