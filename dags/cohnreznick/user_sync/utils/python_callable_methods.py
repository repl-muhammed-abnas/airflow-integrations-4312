import rail

null = None

def create_componentcompant_add_payload():
    component_company_to_add = rail.load_all_records(rail.result('query_company_udf_values_add'))
    current_drop_down_details = rail.result("get_componentcompany_udf_dropdown_values")

    data = current_drop_down_details + component_company_to_add

    def get_payload(item):
        return {
            "target": {
                "uri": item['uri'],
                "name": null
            } if item.get('uri') else null,
            "name": item['name'] if item.get('name') else item['company'],
            "isEnabled": item.get('enabled', 1)
        }

    return list(map(get_payload, data))

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

# pylint: disable=too-many-branches
def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid'],
            'message': log['message']
        },
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records ))))

    return  final_log_records

def get_log_status_or_message(action, user_add_update, task_id):
    exception = rail.result(task_id, "exception")
    msg = f"User {user_add_update}"
    if exception:
        if action == "msg":
            return f"{msg} Partially; {exception}"
        return "Exception"
    if action == "msg":
        return f"{msg} Successfully"
    return "Success"
