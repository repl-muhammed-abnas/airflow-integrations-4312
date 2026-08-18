region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

master_dag_max_active_runs = 1
child_dag_max_active_runs = 10
child_dag_log_generation_max_active_runs = 5

enabled_users_report_name = '***UserUri'

input_filepath = '/Production/User'
archive_filepath = '/Production/Archives/User'

sumo_conn_id = 'sumologic-dagrunlogger'
