region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

sumo_conn_id = 'sumologic-dagrunlogger'
est_timezone = 'Europe/Zurich'  # ChainIQ is based in Switzerland

user_disable_report_name = "User Disable Template Report"
disable_master_dag_active_runs = 1
disable_master_dag_interval = "0 1 * * *"  # Daily at 1 AM
disable_child_dag_active_runs = 5
