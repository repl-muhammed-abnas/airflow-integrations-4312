import rail

def do_format_logs():
    log_artifacts = []
    log_records = []

    timeofflogs = rail.result("create_log")

    if timeofflogs:
        if isinstance(timeofflogs, list):
            log_artifacts.extend(timeofflogs)
        else:
            log_artifacts.append(timeofflogs)

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
