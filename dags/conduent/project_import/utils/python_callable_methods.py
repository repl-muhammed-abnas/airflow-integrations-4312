import rail
import itertools

null = None

MANDATORY_FIELDS = {
    "project_code": "Project ID",
    "project_status": "Project Status",
    "project_name": "Project Name",
    "project_description": "Description",
    "start_date": "Date Opened",
    "project_manager_id": "Project Manager",
    "project_category": "Project Category",
    "project_type": "Project Type",
    "billable_non_billable": "Billable / Non-Billable",
    "cost_center_name": "Cost Center Name",
    "requested_by": "Requested By"
}


def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"Project creation/updation is skipped as the mandatory value {log_value} not present in the input file")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_all_custom_fields_data(response):
    return list(map(lambda i: {
        "displayText": i["displayText"],
        "uri": i["uri"]
    }, response))


def is_projectmanager_permission(response):
    if response:
        if rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:project-management', 'permissionSet'):
            return True
    return False


def get_unique_cost_center_uri(resp, cost_center_name):
    cost_center_uri_list = [item['uri'] for item in resp if item['displayText'] == cost_center_name]
    if len(cost_center_uri_list) > 1:
        return False
    if not cost_center_uri_list:
        return None
    return cost_center_uri_list[0]

def do_format_logs(dag_run):
    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = []
    if dag_run.conf['main_logs']:
        log_artifacts.append(dag_run.conf['main_logs'])

    if dag_run.conf['project_child_logs']:
        log_artifacts.extend(dag_run.conf['project_child_logs'])

    log_records = []

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = list(
        map(
            lambda x: {
                **{k: v for k, v in x["properties"].items()},
                **{"jobid": x["ecid"]},
            },
            log_records,
        )
    )

    rail.set_result(key="success_record_count", val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="error_record_count", val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="exception_record_count", val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))

    return final_log_records

def get_all_triggered_child_for_task_id(config,task_id):
    return list(itertools.chain(
        *list(filter(None, map(lambda x: rail.result(
                    f'{task_id}_{x+1}'), range(config.child_max_active_runs))))))

def get_all_triggered_child_dags_callable(config):
            dag_run_ids = []
            dag_run_ids.extend(get_all_triggered_child_for_task_id(config, 'trigger_dag_run_project_add_async'))
            dag_run_ids.extend(get_all_triggered_child_for_task_id(config, 'trigger_dag_run_project_update_async'))
            return dag_run_ids

def get_supervisor_uri(response, dag_run):
    if not response["rows"]:
        return False
    project_manager = list(filter(lambda i:dag_run.conf["project_manager_id"] == i["cells"][3]["textValue"], response["rows"]))
    if len(project_manager) == 1:
        return project_manager[0]["cells"][0]["uri"]
    return "Multiple Project Managers Found"

