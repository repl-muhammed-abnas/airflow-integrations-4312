import ast
from airflow.models import Variable
import rail


def get_ord_mapper_non_specific_values(ord_department_config):
    ord_mapper_values = ast.literal_eval(
        Variable.get(ord_department_config, default_var=[]))
    return list(filter(lambda x: x["ord"] == "yes" and x["specific"] == "No", ord_mapper_values))


def get_ord_mapper_specific_values(ord_department_config):
    ord_mapper_values = ast.literal_eval(
        Variable.get(ord_department_config, default_var=[]))
    return list(filter(lambda x: x["ord"] == "yes" and x["specific"] == "Yes" and x["level_1"] == "PwC New Zealand", ord_mapper_values))


def get_pwc_ord_structure_from_variable(ord_level, ord_id, instance):
    variable = (ord_level + '_' + ord_id + '_' + instance).replace(' ', '_')
    return Variable.get(variable, default_var=[])


def do_format_logs():

    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = []
    if rail.result('create_ord_department_sync_logs'):
        log_artifacts.append(rail.result('create_ord_department_sync_logs'))

    if rail.result('gather_ord_department_logs_from_child_v1_012'):
        log_artifacts.extend(rail.result(
            'gather_ord_department_logs_from_child_v1_012'))

    if rail.result('gather_ord_department_logs_from_child_v1_028'):
        log_artifacts.extend(rail.result(
            'gather_ord_department_logs_from_child_v1_028'))

    if rail.result('gather_ord_department_logs_from_child_v1_042'):
        log_artifacts.extend(rail.result(
            'gather_ord_department_logs_from_child_v1_042'))

    if rail.result('gather_ord_department_logs_from_child_v1_056'):
        log_artifacts.extend(rail.result(
            'gather_ord_department_logs_from_child_v1_056'))

    if rail.result('gather_ord_department_logs_from_child_v1_070'):
        log_artifacts.extend(rail.result(
            'gather_ord_department_logs_from_child_v1_070'))

    if rail.result('gather_ord_department_logs_from_child_v1_086'):
        log_artifacts.extend(rail.result(
            'gather_ord_department_logs_from_child_v1_086'))

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
