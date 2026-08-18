instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
execution_timeout_days = 14
company_key = f"airflowsandbox{region.replace('-', '')}"
airflow_connector_ui_connid = "airflow_connector_ui_endpoint_zendesk"
hmac_secret = 'airflow_connector_ui_hmac_secret'
replicon_conn_id = 'airflowsandbox-replicon-admin'
can_run_batch_task_var_name = f'zendesk_client_import_{instance}_can_run_batch_task'

max_active_runs = 10

provider = "zendesk"
workflow = "client_import"
