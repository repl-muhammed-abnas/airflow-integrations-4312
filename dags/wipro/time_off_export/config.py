region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
max_active_runs_second_child= 1
execution_timeout_days= 14
log_generation_dag_interval =  "0 */1 * * *"
lookup_log_timestamp_hours:int = 1
can_process_batch_task = "wipro_timeoff_export_can_process_batch_task"
