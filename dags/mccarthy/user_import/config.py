region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

master_dag_max_active_runs = 1
child_dag_max_active_runs = 10

input_filepath = '/Gen3Production/UserImport/Input'
archive_filepath = '/Gen3Production/UserImport/Archive'
log_filepath = '/Gen3Production/UserImport/Logs'

sumo_conn_id = 'sumologic-dagrunlogger'
