region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1

schedule_interval = '*/5 * * * *'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'


client_import = 'client_import'
project_import ='project_import'
workflows = ['client_import','project_import']
connector_name = 'zendesk'
