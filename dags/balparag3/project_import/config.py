region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14

dag_max_active_tasks = 120
master_dag_max_active_runs = 1
child_dag_create_project_max_active_runs = 6
child_dag_process_project_max_active_runs = 6
child_dag_create_client_max_active_runs = 4
child_dag_log_generation_max_active_runs = 2

input_filepath = '/Balpara/balpara.projectimport/Input'
archive_filepath = '/Balpara/balpara.projectimport/Archive'
fromaddress_filepath = '/Balpara/balpara.projectimport/fromaddress'

dagrun_log_conn_id = 'sumologic-dagrunlogger'

time_zone = 'Australia/Melbourne'
