import rail
from datetime import datetime, timedelta, timezone

def check_project_id_hidden_is_not_present(data):
    project_id_hidden = []
    if data.get('records')[0].get("Project_Id_Hidden__c"):
        project_id_hidden.append(data.get('records')[0].get("Project_Id_Hidden__c"))
        return project_id_hidden
    else:
        return project_id_hidden

def check_project_manager_is_not_present(data):
    manager_id_hidden = []
    if data.get('records')[0].get("Project_Manager__c"):
        manager_id_hidden.append(data.get('records')[0].get("Project_Manager__c"))
        return manager_id_hidden
    else:
        return manager_id_hidden
    
def check_length_of_list(result_list):
    return len(result_list)

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

def get_formatted_date(input_date):
    if input_date:
        dt = datetime.strptime(input_date, "%Y-%m-%d")

        return {
            "date": {
                "year": dt.year,
                "month": dt.month,
                "day": dt.day
            }
        }

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

def validate_project_manager_exists(salesforce_result):
    """
    Validate that the project manager was found in Salesforce.

    Args:
        salesforce_result: Result from searchRepliconProjectManagers query

    Returns:
        True if found

    Raises:
        Exception if project manager not found
    """
    records = salesforce_result.get('records', [])
    if not records or len(records) == 0:
        raise Exception("Project manager not found in Replicon_Project_Managers__c custom object in Salesforce")

    return True

def get_status_uri(status_value):
    uri = next(
        (item["uri"] for item in status_value if item.get("displayText") == "In Progress"),
        None
        )
    return uri