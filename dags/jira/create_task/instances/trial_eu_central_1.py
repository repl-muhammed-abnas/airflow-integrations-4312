instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'
execution_timeout_days = 14
child_dag_max_active_runs = 10
company_key = f"airflowsandbox{region.replace('-', '')}"
hmac_secret = 'airflow_connector_ui_hmac_secret'
replicon_conn_id = 'airflowsandbox-replicon-admin'
can_run_batch_task_var_name = f'standard_jira_create_task_{instance}_can_run_batch_task'
provider = 'jira'
workflow = 'create_task'

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_jira'
