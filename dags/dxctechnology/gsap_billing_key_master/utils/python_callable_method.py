import rail
from dxctechnology.gsap_billing_key_master.utils import custom_methods

null = None


def get_valid_wbs_records():
    records = rail.result('get_wbs_records_from_xml')
    return [record for record in records if record['wbs'] and record['taskName']]

def get_blank_wbs_records():
    records = rail.result('get_wbs_records_from_xml')
    return [record for record in records if not record['wbs'] or not record['taskName']]


def project_date_range(task_detail):
    timedaterange = rail.result(task_detail)[
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

    for task in tasks:
        name = task['name']
        code = task['code'] if task['code'] else ''
        tasks_list.append({'name': name,
                           'code': code,
                           'enddate': (str(task['timeEntryDateRange']['endDate']['day']) + '/' + str(task['timeEntryDateRange']['endDate']['month'])
                                       + '/' + str(task['timeEntryDateRange']['endDate']['year'])) if bool(task['timeEntryDateRange']['endDate']) else null,
                           'oef': rail.find_first_by_attr_and_get_attr(task['customFields'], "customField.displayText", "Task Type", "text"),
                           'uri': task['uri']
                           })
    return tasks_list


def retrive_attributes_from_input(tasks_result):
    tasks_list = custom_methods.get_data_from_document(
        rail.result(tasks_result))
    filtered_attributes = {
        'taskName': custom_methods.get_conf()['taskName'],
        'taskCode': custom_methods.get_conf()['taskCode'],
        'action': 'create',
        'attribute1uri': 'NA'
    }
    if len(tasks_list) == 0:
        return filtered_attributes
    for task in tasks_list:
        if task['name'] == filtered_attributes['taskName']:
            if task['code'] == filtered_attributes['taskCode']:
                return []
            filtered_attributes['action'] = 'update'
            return filtered_attributes
    filtered_attributes['attribute1uri'] = tasks_list[0]['uri']
    return filtered_attributes

def is_task_already_present():
    return bool(list(filter(lambda task: task['name'] == rail.result('for_each_billing_key_start')['taskName'],
                            rail.load_all_records(rail.result("query_task_list")))))

def is_task_already_present_child():
    return bool(list(filter(lambda task: task['name'] == rail.result('for_each_billing_key_start')['taskName'],
                            rail.load_all_records(rail.result("query_task_list_child")))))

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    billing_key_logs = dag_run.conf['billing_key_logs']
    skip_logs = dag_run.conf['skip_logs']

    if billing_key_logs:
        if isinstance(billing_key_logs, list):
            log_artifacts.extend(billing_key_logs)
        else:
            log_artifacts.append(billing_key_logs)

    if skip_logs:
        if isinstance(skip_logs, list):
            log_artifacts.extend(skip_logs)
        else:
            log_artifacts.append(skip_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'ecid': log['ecid'],
        },
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'skipped', final_log_records ))))

    return  final_log_records
