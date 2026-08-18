region = 'us-east-1'
environment = 'production'

company_key = 'airflow'
replicon_conn_id = 'airflow-replicon-admin'
http_conn_id = 'mm_replicon'

monitoring_list_var = 'cloudclock_monitoring_alert_list'

child_dag = "cloud_clock_monitoring_alerts_generic_child_dag"

dag_run_schedule = 8
execution_timeout_days = 14

max_active_runs_child_dag = 5
max_active_runs_main_dag = 1
