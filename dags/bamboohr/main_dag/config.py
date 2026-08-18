region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1

schedule_interval = '*/5 * * * *'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

user_import = 'user_import'
disable_user = 'disable_user'
connector_name = 'bamboohr'

workflows = ['user_import', 'disable_user']
