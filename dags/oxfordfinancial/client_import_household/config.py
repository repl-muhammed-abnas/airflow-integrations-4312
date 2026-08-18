region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

master_dag_max_active_runs = 1
child_dag_max_active_runs = 10
child_dag_log_generation_max_active_runs = 5

input_filepath = '/Production/Client'
archive_filepath = '/Production/Archives/Client'
reference_file = '/Production/Client/reference/reference_household/reference_household.csv'

sumo_conn_id = 'sumologic-dagrunlogger'
