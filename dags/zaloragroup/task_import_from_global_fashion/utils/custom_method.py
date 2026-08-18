from datetime import datetime
import math
import rail

def get_replicon_date(date_str, date_format='%Y-%m-%d'):
    if not date_str:
        return None
    # date format in 2006-04-01
    try:
        date = datetime.strptime(date_str, date_format)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def convert_input_data_to_task_data(item):
    if not item:
        return []

    return {
        'issue': item['id'] if item['id'] else None,
        'key': item['key'] if item['key'] else None,
        'self': item['self'] if item['self'] else None,
        'created': item['fields']['created'] if item['fields'] else None,
        'summary': item['fields']['summary'] if item['fields'] else None,
        'status': item['fields']['status']['statusCategory']['name'] if item['fields']['status'] else None,
        #pylint: disable=invalid-character-backspace
        'taskname': (item['key'] + " - " + (item['fields']['summary'].replace("", "")))[slice(0,255)].strip(),
        'projectname': "GFG Development" + " - " + f'{get_replicon_date(item["fields"]["created"][0:10])["year"]}'
    }

def check_task_data(condition):
    task_data = rail.result("get_task_listy_by_code")
    issue_data = rail.result("for_each_issue_key")

    if task_data and condition == 'taskcode':
        issue_check = rail.find_first_by_attr_and_get_attr(task_data, 'Taskcode', issue_data['key'], 'URI')

        return bool(issue_check)

    if task_data and condition == 'taskname':
        issue_check = rail.find_first_by_attr_and_get_attr(task_data, 'Taskname', issue_data['taskname'], 'URI')

        return bool(issue_check)

    return False

def count_of_jira_data(total_records_found):
    max_return = 100
    n = math.ceil(total_records_found/max_return)
    l = []
    for i in range(0, n):
        l.append(i*100)
    return l
