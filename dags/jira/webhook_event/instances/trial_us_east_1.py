instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
execution_timeout_days = 14
master_max_active_runs = 5
child_dag_max_active_runs = 3
company_key = f"airflowsandbox{region.replace('-', '')}"
hmac_secret = 'airflow_connector_ui_hmac_secret'
replicon_conn_id = 'airflowsandbox-replicon-admin'
can_run_batch_task_var_name = f'standard_jira_user_export_{instance}_can_run_batch_task'

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_jira'
