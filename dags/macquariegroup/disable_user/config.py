
region = 'eu-central-1'
environment = 'pre-production'

master_dag_active_runs = 1
child_dag_active_runs = 10
dag_max_active_tasks = 128

execution_timeout_days = 14

master_dag_interval = '0 5 * * *'

sumo_conn_id = 'sumologic-dagrunlogger'

aus_timezone = 'Australia/Sydney'

user_disable_report_name = '***User Template - For Disable'

default_supervisor = "macquarieproduction_default_supervisor_id"
