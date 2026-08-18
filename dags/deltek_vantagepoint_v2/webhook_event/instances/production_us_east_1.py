instance = 'production'
region = 'us-east-1'
environment = 'production'
execution_timeout_days = 14
master_max_active_runs = 5
child_dag_max_active_runs = 3
company_key = f"airflow{region.replace('-', '')}"
replicon_conn_id = 'airflow-replicon-admin'
can_run_batch_task_var_name = f'vp_replicon_user_webhook_{instance}_can_run_batch_task'
can_run_batch_task_project_var_name = f'vp_replicon_project_webhook_{instance}_can_run_batch_task'

webhook_basicauth_username = f'deltek_vantagepoint_webhook_username_{company_key}'
webhook_basicauth_password = f'deltek_vantagepoint_webhook_password_{company_key}'

webhook_username = 'rep_vp_webhook_user'
webhook_password = 'Deltek@123'

connector_name = 'vantagepoint'
workflows = ['user_sync','project_sync']
user_sync = 'user_sync'
project_sync = 'project_sync'



user_webhook_event_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_webhook_event_{instance}'
project_webhook_event_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_project_webhook_event_{instance}'
user_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_sync_main_{instance}'
project_sync_main_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_project_sync_main_{instance}'

# This is only for the UI endpoint of the connector as workaround for connector specific testing
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'
