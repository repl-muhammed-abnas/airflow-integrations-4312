region = 'us-east-1'
environment = 'pre-production'

master_dag_interval = 30

execution_timeout_days = 14

master_dag_active_runs = 1
child_dag_referencefile_active_runs = 10
child_dag_active_runs = 20
child_dag_log_active_runs = 1

dag_max_active_tasks = 200

sumo_conn_id = 'sumologic-dagrunlogger'
