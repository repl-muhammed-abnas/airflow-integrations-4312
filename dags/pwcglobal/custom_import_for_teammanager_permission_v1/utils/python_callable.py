from ast import literal_eval
import rail
import ast
from functools import reduce

null = None

def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def get_status(item, logstatus):
    status = 'status' if item.get('status') else 'Status'
    return item[status].lower() == logstatus

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    supervisory_org_logs = dag_run.conf['supervisory_org_logs']
    otherlogs = dag_run.conf['otherlogs']

    if supervisory_org_logs:
        if isinstance(supervisory_org_logs, list):
            log_artifacts.extend(supervisory_org_logs)
        elif isinstance(supervisory_org_logs, str) and supervisory_org_logs[0] == '[':
            supervisory_org_logs = literal_eval(supervisory_org_logs)
            log_artifacts.extend(supervisory_org_logs)
        else:
            log_artifacts.append(supervisory_org_logs)

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
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{"ecid":log['ecid']},
        **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records


def get_status_and_details(dag_run):
    message = "Success"
    details = "Supervisory Org for Team Manager processed successfully. "
    if rail.result('search_user'):
        if str(rail.result('search_user')[0]['userDetails']['isEnabled']).lower() == 'false':
            details = "Supervisory Org for Team Manager processed successfully for the disabled user. "

    has_exception_message = rail.result('log_supervisory_org_has_more_than_7_levels') if rail.result(
        'log_supervisory_org_has_more_than_7_levels') else rail.result('log_team_manager_permission_not_present') if rail.result(
            'log_team_manager_permission_not_present') else rail.result('log_supervisory_org_level_path_missing') if rail.result(
                'log_supervisory_org_level_path_missing') else rail.result('log_user_not_present') if rail.result(
                    'log_user_not_present') else rail.result('log_user_belong_to_other_location') if rail.result(
                        'log_user_belong_to_other_location') else ''

    if has_exception_message:
        message = "Exception"
        details = has_exception_message
    return {
        "guid": dag_run.conf['guid'],
        "supervisory_org_level": dag_run.conf['supervisory_org'],
        "status": message,
        'details': details
    }

def get_required_supervisory_org_level_uris(response,dag_run):
    if response['rows']:
        return list(map(lambda row: row['cells'][0]['uri'], filter(lambda row: (
            '|'.join(d['textValue'] for d in row['cells'][1]['cellCollection'])
        ).startswith(dag_run.conf['supervisory_org']), response['rows'])))

    return None

def append_supervisory_org_to_logged_data():
    logged_data = rail.load_all_records(rail.result('check_for_current_run_logs'))
    sup_org_uri_list = rail.result('get_assigned_supervisory_org_child_levels_uri')
    if logged_data:
        logged_sup_org = reduce(lambda acc, row: acc + ast.literal_eval(
                    row["properties"]["supervisory_org_level"].strip()
                ),logged_data,[])
        sup_org_uri_list.extend(logged_sup_org)
    return list(set(sup_org_uri_list))
