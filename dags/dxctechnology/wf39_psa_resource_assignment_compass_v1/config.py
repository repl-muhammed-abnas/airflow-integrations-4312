region = 'us-east-2'
environment = 'pre-production'

master_dag_interval = 30
file_sensor_timeout = 10
max_active_run = 1

child_dag_create_billing_rate_max_active_runs = 128
child_dag_process_wbs_max_active_runs = 128
compass_child_dag_process_wbs_max_active_runs = 128

execution_timeout_days = 14

extract_report_name = '**C1 Lean staffing Import base report'
disabled = True
