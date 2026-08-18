region = 'us-east-1'
instance = 'qa'
environment = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowqasandbox-replicon-admin'
webhook_secret = 'airflow_connector_ui_hmac_secret'
can_run_batch_task_var_name = f'airflow_connection_dag_{instance}_can_run_batch_task'
execution_timeout_days = 14
sumo_conn_id = 'sumologic-connector-logger'
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'

duplicate_job_message = 'Another job is already queued or running. Please try again later'
paused_dag_message = 'Workflow is unavailable or paused. Contact Support for more details'
max_active_runs = 20
