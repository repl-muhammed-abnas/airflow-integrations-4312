

import rail
from ast import literal_eval


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    disabled_user_logs = dag_run.conf['disabled_user_logs']
    otherlogs = dag_run.conf['otherlogs']

    if disabled_user_logs:
        if isinstance(disabled_user_logs, list):
            log_artifacts.extend(disabled_user_logs)
        elif isinstance(disabled_user_logs, str) and disabled_user_logs[0] == '[':
            disabled_user_logs = literal_eval(disabled_user_logs)
            log_artifacts.extend(disabled_user_logs)
        else:
            log_artifacts.append(disabled_user_logs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
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
        **{"ecid": log['ecid']},
        **dict(log['properties'].items()),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records


def get_event_identifier_oef_uri(response):
    rail.set_result(key="response", val=response)
    return rail.find_first_by_attr_and_get_attr(response, 'name', 'Event Identifier', 'uri', '')
