region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

sftp_conn_id = "Airflowmig_useast2"

master_dag_interval = 30
file_sensor_timeout = 15
execution_timeout_days = 14

child_dag_process_wbs_max_active_runs = 5
child_dag_process_tasks_max_active_runs = 5
child_dag_process_billing_keys_max_active_runs = 5

child_dag_update_task_max_active_runs = 5
child_dag_create_task_max_active_runs = 5
parallel_run_count = 10

# DO not change unless multiple files needs to be processed at same time
gsap_task_import_master_max_active_runs = 1
disabled = True
