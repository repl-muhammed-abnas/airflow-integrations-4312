import rail
from rail.lib.ecid import get_dagrun_ecid, get_my_ecid


def do_assert_log_entries(filter_task_id, error_message):

    current_context = rail.get_current_context()
    dag_run_context = current_context["dag_run"]
    run_id = current_context["run_id"]

    expected_log_entries = [{
        'ecid': f'{get_my_ecid(current_context)} | {run_id}',
        'source': f'{current_context["dag"].dag_id}/{run_id}/{current_context["task"].task_id}/{current_context["ti"].try_number}',
        'severity': 'Info',
        'message': f"add message for DAG Run ECID {get_dagrun_ecid(dag_run_context)}",
        'properties': dag_run_context.conf
    }]

    actual_log_entries = [{k: v for k, v in item.items() if k != 'timestamp'} for item in rail.load_all_records(
        rail.result(filter_task_id))]

    assert expected_log_entries == actual_log_entries, error_message
