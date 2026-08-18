region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

trigger_parallel_dagrun_count_process_users = 10

sumo_conn_id = 'sumologic-dagrunlogger'
est_timezone = 'Asia/Kolkata'

user_disable_report_name = "***User Disable Template Report ***"
disable_master_dag_active_runs = 1
disable_master_dag_interval = "0 1 * * *"
disable_child_dag_active_runs = 5


