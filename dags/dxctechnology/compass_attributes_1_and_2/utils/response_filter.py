from datetime import date
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods
null = None


def get_success_error_messages(response):
    attributes = custom_methods.get_conf()['data']
    data = response.json()['d']
    success = []
    error = []
    count = 0
    for item in data:
        if not item['error']:
            success.append({
                'attributename': item['task']['name'],
            })
        else:
            error.append({
                'attributename': attributes[count]['name'],
                'message': item['error']['notifications'][0]['displayText']
            })
        count += 1
    return {
        "success": success,
        "error": error
    }


def get_all_child_tasks_for_update(response):
    attribute = custom_methods.get_conf()['data'][0]
    attribute_enddate = date(
        attribute['enddateyear'], attribute['enddatemonth'], attribute['enddateday'])
    data = response.json()['d']

    child_tasks = []
    child_child_tasks = []
    child_child_child_tasks = []

    def checkdate(enddate):
        task_enddate = date(enddate['year'], enddate['month'], enddate['day'])
        return attribute_enddate < task_enddate

    # pylint: disable=too-many-nested-blocks
    for item in data:
        if item['task'] and item['task']['timeEntryDateRange']['endDate'] and item['task']['timeEntryDateRange']['endDate']['day']:
            if checkdate(item['task']['timeEntryDateRange']['endDate']):
                child_tasks.append({
                    'uri': item['task']['uri'],
                    'enddate': custom_methods.get_replicon_date(attribute_enddate.strftime('%Y-%m-%d'), '%Y-%m-%d')
                })

            for child in item['childTasks']:
                if child['task'] and child['task']['timeEntryDateRange']['endDate'] and child['task']['timeEntryDateRange']['endDate']['day']:
                    if checkdate(child['task']['timeEntryDateRange']['endDate']):
                        child_child_tasks.append({
                            'uri': child['task']['uri'],
                            'enddate': custom_methods.get_replicon_date(attribute_enddate.strftime('%Y-%m-%d'), '%Y-%m-%d')
                        })

                for child_child in child['childTasks']:
                    # pylint: disable=line-too-long
                    if child_child['task'] and child_child['task']['timeEntryDateRange']['endDate'] and child_child['task']['timeEntryDateRange']['endDate']['day']:
                        if checkdate(child_child['task']['timeEntryDateRange']['endDate']):
                            child_child_child_tasks.append({
                                'uri': child_child['task']['uri'],
                                'enddate': custom_methods.get_replicon_date(attribute_enddate.strftime('%Y-%m-%d'), '%Y-%m-%d')
                            })

    return {
        'child_tasks': child_tasks,
        'child_child_tasks': child_child_tasks,
        'child_child_child_tasks': child_child_child_tasks
    }
