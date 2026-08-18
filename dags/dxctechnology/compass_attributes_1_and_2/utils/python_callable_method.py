from datetime import date, datetime
import hashlib
import rail
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None


def get_valid_wbs_records(wbsfilter=False, wbs_skiplist=null):
    records = rail.result('get_wbs_records_from_xml')
    return [record for record in records if record['wbs']] if not wbsfilter \
        else [record for record in records if record['wbs'] and record['wbs'] not in wbs_skiplist]


def get_blank_wbs_records():
    records = rail.result('get_wbs_records_from_xml')
    return [record for record in records if not record['wbs']]


def project_date_range():
    timedaterange = rail.result("get_project_details_based_on_wbs")[
        "timeEntryDateRange"]
    return {
        'startdate': (str(timedaterange['startDate']['day']) + '/' + str(timedaterange['startDate']['month'])
                      + '/' + str(timedaterange['startDate']['year'])) if bool(timedaterange['startDate']) else null,
        'enddate': (str(timedaterange['endDate']['day']) + '/' + str(timedaterange['endDate']['month'])
                    + '/' + str(timedaterange['endDate']['year'])) if bool(timedaterange['endDate']) else null
    }


def retrieve_task_list(task_details):
    tasks = rail.result(task_details)
    tasks_list = []

    def format_date(enddate):
        if enddate and enddate['day']:
            datestr = str(enddate['year']) + '/' + \
                str(enddate['month']) + '/' + str(enddate['day'])
            return datetime.strptime(datestr, '%Y/%m/%d').strftime('%Y%m%d')
        return ''

    for task in tasks:
        name = task['name']
        code = task['code'] if task['code'] else ''
        enddate = format_date(task['timeEntryDateRange']['endDate'])
        md5 = hashlib.md5(
            (name + ',' + code + ',' + enddate).encode('utf-8')).hexdigest()
        tasks_list.append({'name': name,
                           'code': code,
                           'enddate': (str(task['timeEntryDateRange']['endDate']['day']) + '/' + str(task['timeEntryDateRange']['endDate']['month'])
                                       + '/' + str(task['timeEntryDateRange']['endDate']['year'])) if bool(task['timeEntryDateRange']['endDate']) else null,
                           'oef': rail.find_first_by_attr_and_get_attr(task['customFields'], "customField.displayText", "Task Type", "text"),
                           'uri': task['uri'],
                           'md5': md5
                           })
    return tasks_list


def retrive_attributes_from_input(attribute_result, tasks_result):
    attributes_list = custom_methods.get_data_from_document(
        rail.result(attribute_result))
    tasks_list = custom_methods.get_data_from_document(
        rail.result(tasks_result))
    filtered_attributes = list(
        map(lambda x: {'number': x['AttributeNumber'],
                       'name': x['Attribute'],
                       'code': x['Description'],
                       'enddate': x['EndDate'],
                       'tasktype': x['tasktypeoptionuri'],
                       'md5': x['md5'],
                       'uri': null,
                       'enddatestatus': x['enddatestatus'],
                       'descriptionstatus': x['descriptionstatus']
                       }, attributes_list
            ))
    if len(tasks_list) == 0:
        return filtered_attributes

    attributes_to_process = []
    for attribute in filtered_attributes:
        for task in tasks_list:
            if attribute['name'] and task['name'] == attribute['name']:
                attribute['uri'] = task['uri']
        attributes_to_process.append(attribute)
    return attributes_to_process


def check_key_values_present_in_list(collection, key):
    data = custom_methods.get_data_from_document(
        rail.result(collection))
    return len(list(map(lambda x: {key: x[key]}, data))) > 0


def retrieve_created_task_list(dag_run):
    data = rail.result("get_task_copy_batch_results")["tasks"]
    templist = list(
        map(lambda x: {'name': x['name'],
                       'code': x['description'],
                       'uri': null,
                       'enddateday': custom_methods.get_lower_date(dag_run.conf['projectenddate'], x['enddate'], '%d/%m/%Y', '%Y%m%d')['day'],
                       'endatemonth': custom_methods.get_lower_date(dag_run.conf['projectenddate'], x['enddate'], '%d/%m/%Y', '%Y%m%d')['month'],
                       'enddateyear': custom_methods.get_lower_date(dag_run.conf['projectenddate'], x['enddate'], '%d/%m/%Y', '%Y%m%d')['year']
                       }, dag_run.conf['data']
            ))
    if len(data) == 0:
        return templist

    created_task_list = []
    for task1 in templist:
        for task2 in data:
            if task1['name'] == task2['name']:
                task1['uri'] = task2['uri']
        created_task_list.append(task1)
    return created_task_list


def retrieve_first_task(query_result):
    data = custom_methods.get_data_from_document(
        rail.result(query_result))
    return {
        'number': data[0]['number'],
        'name': data[0]['name'],
        'code': data[0]['code'],
        'enddate': data[0]['enddate'],
        'tasktype': data[0]['tasktype']
    }


def query_result_for_copy_batch(task_list, exclude_first_task, dag_response, collection):
    if not check_key_values_present_in_list(task_list, 'uri'):
        return {
            'task_collection': exclude_first_task,
            'sourcetask': rail.result(dag_response)[0]['uri']
        }
    return {
        'task_collection': collection,
        'sourcetask': custom_methods.get_data_from_document(
            rail.result(task_list))[0]['uri'] if custom_methods.get_data_from_document(
            rail.result(task_list)) else null
    }


def get_attributescount():
    wbsrecords = rail.result('filter_valid_wbs_records')
    count = 0
    for wbsrecord in wbsrecords:
        count += len(wbsrecord['attributes'])
    return count


def get_attributes_status_list(dag_run):
    def check_enddate(enddate):
        if not custom_methods.get_replicon_date(enddate):
            return False
        if not rail.result('get_project_date_range')['startdate']:
            return True

        task_enddate = custom_methods.get_replicon_date(enddate)
        project_startdate = custom_methods.get_replicon_date(
            rail.result('get_project_date_range')['startdate'], '%d/%m/%Y')
        return date(task_enddate['year'], task_enddate['month'], task_enddate['day']) >= \
            date(project_startdate['year'],
                 project_startdate['month'], project_startdate['day'])

    return list(map(lambda item: {
        'AttributeNumber': item['AttributeNumber'],
        'Attribute': item['Attribute'],
        'Description': item['Description'],
        'EndDate': item['EndDate'],
        'tasktypeoptionuri': item['tasktypeoptionuri'],
        'md5': item['md5'],
        'enddatestatus': '' if not item['EndDate'] else 'valid' if check_enddate(item['EndDate']) else 'invalid',
        'descriptionstatus': 'valid' if len(item['Description']) < 50 else 'invalid'
    }, dag_run.conf['attributes']))
