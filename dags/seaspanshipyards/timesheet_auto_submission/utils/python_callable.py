import rail
from rail import get_current_context, load_all_records
from rail.lib.ecid import get_dagrun_ecid


def check_force_approvable():
    if rail.result("status") == "Approve" and len(rail.result("get_expected_approvers")) == 1 \
            and rail.result("get_expected_approvers")[0]["displayText"] == "System":
        return True
    return False


def check_force_approvable_by_anyone():
    if rail.result("status") == "Approve" and len(rail.result("get_expected_approvers")) > 1:
        return True
    return False


def check_submittable():
    if rail.result("status") == "Submit" and len(rail.result("get_expected_approvers")) > 1:
        return True
    return False

def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def load_child_logs():

    dag_run = get_current_context()['dag_run']

    child_log_artifacts = rail.result('gather_log')
    child_log_records = []

    if child_log_artifacts:
        for log in child_log_artifacts:
            log_records = load_records(log)
            if log_records:
                child_log_records.extend(log_records)

    return list(map(lambda x: {
        **dict(x['properties'].items()),
        **{
            'jobid': get_dagrun_ecid(dag_run)
        }}, child_log_records))

def check_logs_size():
    return bool(len(rail.result("format_logs")) > 0)
