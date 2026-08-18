region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1

schedule_interval = '*/5 * * * *'
initial_setup_interval_hours = 24

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

initial_setup = 'initial_setup'
user_sync = 'user_sync'
project_sync = 'project_sync'
time_sync = 'time_sync'
connector_name = 'computerease'

workflows = ['initial_setup', 'user_sync', 'project_sync', 'time_sync']
