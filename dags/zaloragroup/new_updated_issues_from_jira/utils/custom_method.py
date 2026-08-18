import math
import rail
from zaloragroup.new_updated_issues_from_jira.utils.request_payload import get_task_name

def convert_input_data_to_task_data(item):
    if not item:
        return []

    return {
        'key': item['key'] if item['key'] else None,
        'summary': item['fields']['summary'] if item['fields']['summary'] else None,
        'created': item['fields']['created'] if item['fields']['created'] else None
    }

def count_of_jira_data(total_records_found):
    max_return = 100
    n = math.ceil(total_records_found/max_return)
    l = []
    for i in range(0, n):
        l.append(i*100)
    return l

def check_task_data(condition):
    task_data = rail.result("get_task_listy_by_code")
    issue_data = rail.result("for_each_issue_key")

    if task_data and condition == 'taskcode':
        issue_check = rail.find_first_by_attr_and_get_attr(task_data, 'Taskcode', issue_data['key'], 'URI')

        return bool(issue_check)

    if task_data and condition == 'taskname':
        issue_check = rail.find_first_by_attr_and_get_attr(task_data, 'Taskname', get_task_name(
            issue_data['key'], issue_data['summary']), 'URI')

        return bool(issue_check)

    return False
