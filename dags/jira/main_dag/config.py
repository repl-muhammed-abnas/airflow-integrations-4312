region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1

schedule_interval = '*/5 * * * *'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'


close_task = 'close_task'
create_task = 'create_task'
project_import = 'project_import'
create_user = 'create_user'
user_export = 'user_export'
workflows = ['close_task','create_task','project_import','create_user','user_export']
connector_name = 'jira'
