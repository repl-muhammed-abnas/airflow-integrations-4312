region = 'us-east-2'
environment = 'pre-production'

master_dag_interval = 30

master_dag_active_runs = 1
child_dag_user_profile_active_runs = 10
dag_max_active_tasks = 128

execution_timeout_days = 14

delimiter = '|'

sumo_conn_id = 'sumologic-dagrunlogger'
should_add_emailaddress = True
