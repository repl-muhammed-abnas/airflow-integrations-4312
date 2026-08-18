import math
from datetime import datetime
import rail


def convert_input_data_to_task_data(item):
    if not item:
        return []

    res = {
        'key': item['key'],
        'projectid': item['fields']['project']['id'],
        'programname': item['fields']['project']['name'],
        'projectkey': item['fields']['project']['key'],
        'status': item['fields']['status']['name'],
        'type': item['fields']['issuetype']['name'],
        'summary': item['fields']['summary'],
        'startdate': item['fields']['customfield_10037'],
        'enddate': item['fields']['customfield_10034'],
        'assignee': item['fields']['assignee']['accountId'] if item['fields']['assignee'] else None
    }

    return {k: v if v is not None else '' for k, v in res.items()}

def convert_input_data_to_subtask_data(item):
    if not item:
        return []

    res = {
        'subtask_key': item['key'],
        'parent_key': item['fields']['parent']['key'],
        'startdate': item['fields']['customfield_10037'],
        'enddate': item['fields']['customfield_10034'],
        'status': item['fields']['status']['name'],
        'summary': item['fields']['summary']
    }

    return {k: v if v is not None else '' for k, v in res.items()}

def check_task_data(dag_run):
    tasks_data= rail.result("get_all_project_tasks")

    return list(filter(lambda item: item['name']== dag_run.conf['summary'], tasks_data)) if tasks_data else None

def get_task_data(dag_run):
    data= rail.result("check_task_in_replicon")
    status = rail.result("serach_project_in_replicon")['status']['name']
    return [{
        'taskname': data[0]['name'] if data else dag_run.conf['summary'],
        'startdate': dag_run.conf['startdate'],
        'enddate': dag_run.conf['enddate'],
        'taskcode': dag_run.conf['task_code'],
        'status': 'Done' if status == 'Completed' else dag_run.conf['status']
    }]

def get_resource_data(dag_run):
    resources = [
        dag_run.conf["resource1"] if dag_run.conf["resource1"] else None,
        dag_run.conf["resource2"] if dag_run.conf["resource2"] else None,
        dag_run.conf["resource3"] if dag_run.conf["resource3"] else None,
        dag_run.conf["resource4"] if dag_run.conf["resource4"] else None,
        dag_run.conf["resource5"] if dag_run.conf["resource5"] else None,
        dag_run.conf["resource6"] if dag_run.conf["resource6"] else None,
    ]

    return [resource for resource in resources if resource is not None]


def convert_data_to_task_details(item,dag_run):
    if not item:
        return []

    res = {
        'taskname': item['fields']['summary'],
        'taskcode': item['key'],
        'startdate': item['fields']['customfield_10037'],
        'enddate': item['fields']['customfield_10034'],
        'status': dag_run.conf['status'] if dag_run.conf['status'] == "Done" else item['fields']['status']['name']
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def count_of_jira_data(total_records_found):
    max_return = 100
    n = math.ceil(total_records_found/max_return)
    l = []
    for i in range(0, n):
        l.append(i*100)
    return l


def get_replicon_date(date_str, date_format='%Y%m%d'):
    if not date_str:
        return None
    # date format in 20060401
    try:
        date = datetime.strptime(date_str, date_format)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None
