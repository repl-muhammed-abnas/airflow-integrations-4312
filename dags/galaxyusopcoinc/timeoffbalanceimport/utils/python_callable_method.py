import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_user_details():
    dag_run_conf = get_dag_run_conf()
    return{
        'useruri': rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"), 'employeeid', dag_run_conf['employeeid'], 'uri'),
        'name': rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"), 'employeeid', dag_run_conf['employeeid'], 'name'),
        'status': rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"), 'employeeid', dag_run_conf['employeeid'], 'status')
    }


def get_enabled_timeoff_uris():
    data = rail.result('get_user_time_off_policy_summary')

    result = list(filter(lambda x: bool(x['isTimeOffAllowedAgainstThisTimeOffType']), map(lambda x: {
        "timeOffTypeuri": x['timeOffTypeuri'],
        "isTimeOffAllowedAgainstThisTimeOffType": x['isTimeOffAllowedAgainstThisTimeOffType']
    }, data)))

    return list(map(lambda x: x['timeOffTypeuri'], result))


def get_user_time_off_types():
    dag_run_conf = get_dag_run_conf()
    data = get_data_from_document(rail.result('query_employee_data'))

    return  list(filter (lambda x: x['timeoffuri'] is not None ,map(lambda item: {
        'timeoffuri': rail.find_first_by_attr_and_get_attr(dag_run_conf['timeoffdetails'], 'description', item['referenceid'], 'uri'),
        'status': rail.find_first_by_attr_and_get_attr(dag_run_conf['timeoffdetails'], 'description', item['referenceid'], 'enabled'),
        'referenceid': item['referenceid']
    }, data)))


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

    employeelogs = dag_run.conf['employeelogs']
    masterlogs = dag_run.conf['masterlogs']

    if employeelogs:
        if isinstance(employeelogs, list):
            log_artifacts.extend(employeelogs)
        else:
            log_artifacts.append(employeelogs)

    if masterlogs:
        if isinstance(masterlogs, list):
            log_artifacts.extend(masterlogs)
        else:
            log_artifacts.append(masterlogs)

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
