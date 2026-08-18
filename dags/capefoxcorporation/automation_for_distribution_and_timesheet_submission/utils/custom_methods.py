from pendulum import now
import rail


def get_run_date_datetime(config):
    today = now(config.time_zone)
    return {
        'day': int(today.day),
        'date': today.strftime(config.DATE_FORMAT),
        'datetime': today.isoformat()
    }


def do_format_logs():
    log_artifacts = []
    log_records = []

    main_logs = rail.result('create_main_log')
    timesheet_submission_logs = rail.result('gather_logs')

    if timesheet_submission_logs:
        if isinstance(timesheet_submission_logs, list):
            log_artifacts.extend(timesheet_submission_logs)
        else:
            log_artifacts.append(timesheet_submission_logs)

    if main_logs:
        if isinstance(main_logs, list):
            log_artifacts.extend(main_logs)
        else:
            log_artifacts.append(main_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid'],
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))

    return final_log_records
