import rail


def psa_parent_org_unit_uri():
    return rail.find_first_by_attr_and_get_attr(rail.result('get_all_org_units'), 'name', 'PSA Org Unit', 'uri')

def get_status(dag_run):
    return 'Exception' if rail.find_first_by_attr_and_get_attr(dag_run.conf['current_organization_unit_parent'], 'textValue',
                                                             'PSA Org Unit', 'textValue') else "Success"

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = [rail.result("create_log")] + rail.result("gather_process_psa_org_units_logs")

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        }, log_records))

    rail.set_result(key="get_logged_success", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_logged_skipped", val=len(list(filter(lambda item: item['status']=="Skipped", final_log_records))))

    return final_log_records
