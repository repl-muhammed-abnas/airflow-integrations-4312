instance = 'production'
region = 'eu-central-1'
environment = 'production'
execution_timeout_days = 14
child_dag_max_active_runs = 10
company_key = f"airflow{region.replace('-', '')}"
hmac_secret = 'airflow_connector_ui_hmac_secret'
replicon_conn_id = 'airflow-replicon-admin'
can_run_batch_task_var_name = f'standard_jira_project_import_{instance}_can_run_batch_task'
provider = 'jira'
workflow = 'project_import'

main_dag_id = f"standard_jira_{region.replace('-', '_')}_project_import_{instance}"
project_import_child_dag_id = f"standard_jira_{region.replace('-', '_')}_project_import_child_dag_{instance}"
hierarchy_sync_child_dag_id = f"standard_jira_{region.replace('-', '_')}_hierarchy_sync_child_dag_{instance}"
create_user_child_dag_id = f"standard_jira_{region.replace('-', '_')}_create_user_child_dag_{instance}"

replicon_level_to_hierarchy = {
    'project': 0,
    'task': 1,
    'subTaskLevel1': 2,
    'subTaskLevel2': 3,
}

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
