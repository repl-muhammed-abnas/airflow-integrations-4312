region = 'us-east-2'
environment = 'pre-production'

extract_report_name = '**C1 Lean staffing Import base report'

# DAG Configuration
master_dag_max_active_runs = 1
child_dag_max_active_runs = 10
parallel_dagrun_count_each_wbs_attribute = 10
execution_timeout_days = 14
gather_logs_timeout_hours = 12
