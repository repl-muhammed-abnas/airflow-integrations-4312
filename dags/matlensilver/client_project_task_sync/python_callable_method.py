import rail


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_client_errors_status():
    if get_task_state('log_client_success') != 'success':
        return get_data_from_document(rail.result('create_client_logs'))
    return []


def get_project_success_status():
    return get_data_from_document(rail.result('create_project_logs'))


def get_task_success():
    return get_data_from_document(rail.result('create_task_logs'))


def get_client_success_status():
    if get_task_state('log_client_success') == 'success':
        return get_data_from_document(rail.result('create_client_logs'))
    return []


def get_project_count():
    data = rail.result('get_project_status')
    update_count = 0
    add_count = 0
    if rail.result('get_project_status'):
        add_count = len(
            list(filter(lambda item: item['severity'] == 'Project_Added', data)))
        update_count = len(
            list(filter(lambda item: item['severity'] == 'Project_Updated', data)))

    return {'add_count': add_count, 'update_count': update_count}


def get_task_count():
    data = rail.result('get_task_status')
    update_count = 0
    add_count = 0
    if rail.result('get_project_status'):
        add_count = len(
            list(filter(lambda item: item['severity'] == 'Task_Added', data)))
        update_count = len(
            list(filter(lambda item: item['severity'] == 'Task_Updated', data)))

    return {'add_count': add_count, 'update_count': update_count}


def get_log_task_state():
    if get_task_state('log_resource_not_in_replicon') == 'success':
        return 'Person ID not available/disabled in Replicon,Not added to WBS'
    return 'skipped'
