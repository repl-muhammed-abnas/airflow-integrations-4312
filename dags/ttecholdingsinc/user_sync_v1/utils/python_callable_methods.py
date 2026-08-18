from operator import itemgetter
import rail


def get_required_time_off_type_details(required_timeoff_types_names):
    log_time_off_type_exception = []
    exception_message = ""
    data = rail.result('get_all_time_off_types')
    all_time_off_types_names = list(map(itemgetter('timeoff_type_name'), data))

    for item in required_timeoff_types_names:
        if item not in all_time_off_types_names:
            log_time_off_type_exception.append(item)

    if log_time_off_type_exception:
        exception_message = f"Time off Type - '{rail.smartjoin_by_delim(log_time_off_type_exception,',')}' not available in Replicon"

    return {"time_off_type_exception_log": exception_message if log_time_off_type_exception else [],
            "result": list(filter(lambda time_off: time_off['timeoff_type_name'] in list(set(required_timeoff_types_names)), data))}


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
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **dict(log['properties'].items()),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records
