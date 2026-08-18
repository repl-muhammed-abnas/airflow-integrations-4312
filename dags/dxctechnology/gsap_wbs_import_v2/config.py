region = 'us-east-2'
environment = 'pre-production'

master_dag_interval = 30
max_active_runs_master = 1
child_execution_timeout_hours = 12
gather_each_wbs_logs_timeout_hours = 2
child_wait_execution_timeout_days = 14
execution_timeout_days = 14
trigger_parallel_dagrun_count_project = 20
trigger_parallel_dagrun_count_client = 10

max_active_runs_process_clients = 250
max_active_runs_process_projects = 250
max_active_runs_process_child_projects = 250
max_active_runs_process_create_task = 250
max_active_runs_process_iwo_element = 250
max_active_runs_process_blob = 250
max_active_runs_process_project_type = 250
max_active_run_log_generation = 1

contractor_company_codes = ['AUES', '3001', '3124', '1602', '3118']

base_report_name= "****Aus Contractor Users base report"
expected_report_columns = """user_uri,perner,Employee Type (Current) (Full Path),Company Code (Current)"""
