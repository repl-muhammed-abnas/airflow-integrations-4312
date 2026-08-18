import rail
from dxctechnology.gsap_task_import_project_fields_v2.utils.request_payload import get_date_from_str_date

def create_task_name(task_name, task_code):
    return task_name + (' - ' + task_code if task_code else '')

def get_missing_wbs_length():
    missing_wbs = rail.result('get_records_missing_wbs_from_xml')
    return len(missing_wbs)


def get_iwo_wbs_element(project_data):
    jsonValue = rail.result(project_data)[0]['extensionFieldValue']
    return list(filter(lambda x: x['definition']['displayText'] == "IWO WBS Element", jsonValue))

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

def do_format_task_logs(task_id):
    log_artifacts = rail.result(task_id)
    log_records = []
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)
    return log_records

def do_format_wbs_logs(dag_run):
    log_artifacts:list = dag_run.conf['wbs_logs']
    master_dag_log = dag_run.conf['master_dag_log']
    log_artifacts.append(master_dag_log)
    log_records = []
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    rail.set_result(key="get_successful_attribute_logs", val=len(list(filter(lambda item: item['properties']['status']=="Success", log_records))))
    rail.set_result(key="get_errored_attribute_logs", val=len(list(filter(lambda item: item['properties']['status']=="Error", log_records))))
    rail.set_result(key="get_exception_attribute_logs", val=len(list(filter(lambda item: item['properties']['status']=="Exception", log_records))))

    return log_records

def is_task_found(feed_file_task, assigned_task_list):
    task_name =  create_task_name(feed_file_task['task_name'], feed_file_task['task_code'])
    return rail.find_first_by_attr_and_get_attr(assigned_task_list, 'name', task_name)

def validate_start_end_date(task):
    return (get_date_from_str_date(task['task_end_date']) <
            get_date_from_str_date(task['task_start_date']))

def get_task_to_add_callable(dag_run):
    feed_file_tasks = rail.load_all_records(rail.result('query_gsap_task_records_for_wbs'))
    currently_assigned_tasks_for_project = rail.result('get_all_assigned_gsap_task_for_project')
    all_gsap_tasks_from_replicon = rail.load_json_artifact(dag_run.conf['get_all_gsap_tasks_from_replicon'])

    def get_task_details(task_name, task_code):
        return rail.find_first_by_attr_and_get_attr(all_gsap_tasks_from_replicon,
                                                    'name', create_task_name(task_name, task_code),
                                                    default={})

    task_data = list(map( lambda feed_file_task: {
        **feed_file_task,
        **{
            "is_task_already_assigned": is_task_found(feed_file_task, currently_assigned_tasks_for_project),
            "replicon_oef_task_details": get_task_details(feed_file_task['task_name'], feed_file_task['task_code']),
            # It will be True for start_date > end_date
            "is_valid_task_dates": validate_start_end_date(feed_file_task)
        }
    }, feed_file_tasks))

    rail.set_result(key="task_records_to_update", val=rail.write_json_artifact(list(filter(lambda task: task['is_task_already_assigned'] and
                                                                    task['is_valid_task_dates'] is False, task_data))))
    rail.set_result(key="invalid_date_task_records",
                    val=rail.write_json_artifact(list(filter(lambda t: t['is_valid_task_dates'] is True, task_data))))

    tasks_to_add = list(filter(lambda task: ((not task['is_task_already_assigned'])
                                        and (task['is_valid_task_dates'] is False)), task_data))
    task_to_disable =  []
    for task_name in list(set(map(lambda t: t['task_name'],tasks_to_add))):
        # pylint: disable=cell-var-from-loop
        task_to_disable.extend(list(filter(lambda rep_task: rep_task['actual_name'] == task_name
                                                ,currently_assigned_tasks_for_project)))
    rail.set_result(key="task_to_disable", val=rail.write_json_artifact(task_to_disable))
    return rail.write_json_artifact(tasks_to_add)


def set_dag_run_conf_ancestry(ancestry, context):
    if len(ancestry) > 10:
        context['dag_run'].conf['_ancestry'] = ancestry[:10] + [ancestry[-1]]
        return ancestry[10:-1]
    context['dag_run'].conf['_ancestry'] = ancestry
    return []

def get_process_unique_wbs_conf_reprocess(item, context):
    truncated_ancestry = set_dag_run_conf_ancestry(item['properties']['_ancestry'], context)
    item['properties']['reprocess_count'] = int(item['properties'].get('reprocess_count', 0)) + 1
    return {
        **item['properties'],
        **{
            "truncated_ancestry": truncated_ancestry
        }
    }
