import rail

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


def set_dag_run_conf_ancestry(ancestry, context):
    context['dag_run'].conf['_ancestry'] = ancestry

def get_process_unique_wbs_conf_reprocess(item, context):
    set_dag_run_conf_ancestry(item['properties']['_ancestry'], context)
    item['properties']['reprocess_count'] = int(item['properties'].get('reprocess_count', 0)) + 1
    return {**item['properties']}
