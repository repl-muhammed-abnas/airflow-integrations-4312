import rail

GROUPS_DELIMITER = "|"

def get_department_names():
    data = rail.load_all_records(rail.result('query_distinct_departments_from_validrecords'))
    return list(map(lambda item: {
        "department_full_path": item['department'],
        "department_name": item['department'].split(GROUPS_DELIMITER)[-1],
        "department_full_path_length": len(item['department'].split(GROUPS_DELIMITER))
    }, data))

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


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
            'jobid': log['ecid']
        },
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records ))))

    return  final_log_records

def get_product_licenses_uris(dag_run):
    assigned_licenses = rail.result('get_assigned_product_licenses')
    licenses = dag_run.conf['productlicenceuri']

    if len(assigned_licenses) != len(licenses):
        return [item['uri'] for item in licenses]

    for item in licenses:
        if not rail.find_first_by_attr_and_get_attr(assigned_licenses, 'displayText', (item['name']).lower(),'uri'):
            return [item['uri'] for item in licenses]
    return []
