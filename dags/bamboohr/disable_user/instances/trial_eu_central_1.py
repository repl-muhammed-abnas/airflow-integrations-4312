instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
execution_timeout_days = 14
child_dag_max_active_runs = 10
replicon_conn_id = 'airflowsandbox-replicon-admin'
hmac_secret = 'airflow_connector_ui_hmac_secret'
can_run_batch_task_var_name = f'standard_bamboohr_disable_user_{instance}_can_run_batch_task'
provider = 'bamboohr'
workflow = 'disable_user'

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_bamboohr'
