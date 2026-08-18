region = 'us-east-1'
environment = 'pre-production'

pgp_conn_id = "pgp_vialto_partners"
master_dag_interval = 30
max_active_runs_master = 1
max_active_runs_process_log_generation = 1
max_active_runs_process_assignee_ids_add = 10
max_active_runs_process_assignee_ids_update = 10
max_active_runs_process_each_project = 10
max_active_runs_process_each_replicon_client = 10
max_active_runs_process_get_assignee_details= 20
child_execution_timeout_hours = 12
child_wait_execution_timeout_days = 14

trigger_parallel_dagrun_get_assignee_details = 10
trigger_parallel_dagrun_process_each_replicon_client = 10
trigger_parallel_dagrun_process_each_project = 10

client_report_name = 'Client Details Report'

BATCH_SIZE = 1000
BATCH_SIZE_CLIENT = 3
BATCH_SIZE_PROJECT = 3
