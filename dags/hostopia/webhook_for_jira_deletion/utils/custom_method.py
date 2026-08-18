import rail


def convert_input_data_to_task_data(item):
    if not item:
        return []

    res = {
        'key': item['key'],
        'subtaskstatus': item['fields']['issuetype']['subtask'],
        'projectname': item['fields']['parent']['fields']['summary'] if item['fields']['issuetype']['subtask'] else item['fields']['summary'],
        'taskname': item['fields']['summary'] if item['fields']['issuetype']['subtask'] else None,
        'parent': item['fields']['parent']['key'] if item['fields']['issuetype']['subtask'] else None
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def check_status():
    x = rail.result("get_project_in_replicon_for_subtask")["status"]["name"]
    result= None
    if x in ["Completed", 'Archived']:
        result= False
    if x in ["In Progress"]:
        result= True
    return result
