region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"

master_max_active_run = 1
max_active_run_child = 3
max_active_run_sub_child = 3

execution_timeout = 14

disable_schedule_interval = "0 0 * * *"
log_schedule_interval = "0 0,2,4,6,8,10,12,14,16,18,20,22 * * *"

log_aggregate_hours = 2

can_process_payload_var = "wipro_user_import_can_process_payload"
can_process_batch_task = "wipro_user_import_can_process_batch_task_por"
USER_TEMPLATE_NAME = "Onboarding_New_Entity"
