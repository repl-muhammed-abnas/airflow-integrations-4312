from datetime import datetime, timedelta
from rail import get_current_context, load_all_records

null = None


def create_date_range_seq(end_date_str, start_date_str):
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    days_difference = (end_date - start_date).days
    return list(map(lambda day_index: {'seq': day_index}, range(1, days_difference+1)))


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():
    dag_run = get_current_context()['dag_run']
    log_artifacts = []

    if dag_run.conf['weekly_shift_log']:
        log_artifacts.append(dag_run.conf['weekly_shift_log'])

    if dag_run.conf['child_log']:
        log_artifacts.extend(dag_run.conf['child_log'])

    log_records = []
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)

            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **{k: v for k, v in x['properties'].items() if k != 'email'},
        **{
            'jobid': x['ecid']
        }}, log_records))
