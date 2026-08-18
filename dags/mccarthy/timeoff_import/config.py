region = 'us-east-1'
environment = 'pre-production'
company_key = 'McCarthyafmig'
replicon_conn_id = 'mccarthyafmig_replicon_uuser'
max_active_runs = 1
max_active_runs_child = 5

execution_timeout_days = 14
master_dag_interval = 30
schedule_interval = 30
file_sensor_timeout = 10
timeoff_balance_report = '***TimeOffBalance***'

trigger_parallel_dagrun_count = 30
