from datetime import datetime
import rail
import pendulum

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    logs = dag_run.conf['logs']

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
        **{
            'ecid': log['ecid']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'ignored', final_log_records ))))

    return final_log_records

def get_email_log_details(dag_run):
    current_time = pendulum.now()
    start_time_str = dag_run.conf['start_time']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f%z')).total_seconds() / 60, 1),
        "log_file_name": dag_run.conf['log_filename'],
    }