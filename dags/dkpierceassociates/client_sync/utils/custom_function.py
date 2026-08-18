import rail
from datetime import datetime, timedelta, timezone
from dkpierceassociates.client_sync import config

def check_client_manager_hidden_is_not_present(data):
    client_manager = []
    if data.get('records')[0].get("Client_Manager__c"):
        client_manager.append(data.get('records')[0].get("Client_Manager__c"))
        return client_manager
    else:
        return client_manager

def check_client_id_hiden_is_not_present(data):
    client_id_hidden = []
    if data.get('records')[0].get("Client_Id_Hiden__c"):
        client_id_hidden.append(data.get('records')[0].get("Client_Id_Hiden__c"))
        return client_id_hidden
    else:
        return client_id_hidden


def last_sync_time(last_sync_var):
    sync_time = (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    return rail.get_lastsync_time_variable(
        variable_name= last_sync_var,
        date_format='%Y-%m-%dT%H:%M:%SZ',
        initial_sync_time= sync_time,
        reset_after_threshold=False
        )

def update_last_sync(update_sync_time):
    return rail.set_lastsync_time_variable(
            variable_name= update_sync_time,
            value_to_set= rail.result('get_last_sync_time')['current_time']
        )

def validate_all_child_dags_succeeded(child_dag_runs):
    """
    Validate that all child DAG runs completed successfully.

    Args:
        child_dag_runs: List of child DAG run information from WaitForDagRunsSensor

    Returns:
        True if all succeeded

    Raises:
        Exception if any child DAG failed
    """
    if not child_dag_runs:
        return True

    failed_runs = []
    for dag_run in child_dag_runs:
        state = dag_run.get('state', 'unknown')
        if state not in ['success', 'skipped']:
            failed_runs.append({
                'dag_id': dag_run.get('dag_id'),
                'run_id': dag_run.get('run_id'),
                'state': state
            })

    if failed_runs:
        raise Exception(f"Child DAG runs failed: {failed_runs}")

    return True

def get_country_uri_replicon(country_name, country_list):
    match = next((c['uri'] for c in country_list if c['name'] == country_name), None)
    return {"value": {"uri": match}} if match else None