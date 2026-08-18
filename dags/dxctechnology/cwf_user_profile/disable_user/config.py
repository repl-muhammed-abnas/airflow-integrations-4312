region = 'us-east-2'
environment = 'pre-production'

master_dag_active_runs = 1
child_dag_active_runs = 30
dag_max_active_tasks = 128

execution_timeout_days = 14

master_dag_interval = '0 1 * * *'

sumo_conn_id = 'sumologic-dagrunlogger'

pacific_timezone = 'America/Los_Angeles'

user_contractors_report_name = '***User Template - For Contractors'
disabled = True
