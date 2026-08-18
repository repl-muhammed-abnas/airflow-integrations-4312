region = 'us-east-2'
environment = 'pre-production'

# DO NOT UPDATE THE BATCH_SIZE
# Actual max limit = 1500
PROJECT_DEPENDANT_OEF_ADD_LIMIT = 200

time_zone = "utc"
schedule_interval = "0 */2 * * *"
first_delta = 3
second_delta = 0.5

reprocess_wbs_log_name = "gsap_project_field_task_import_reprocess_log_wbs_v1"
master_dag_max_active_runs=1

master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_days = 14
execution_timeout_hours = 5
child_dag_sync_gsap_task_max_active_runs = 10
max_active_run_log_generation = 1
child_dag_sync_gsap_task_system_level = 10

child_dag_sync_each_attribute_project_level_max_active_runs = 10
# process_child_wbs
child_wbs_dag_sync_gsap_task_max_active_runs = 10

max_active_run_move_to_processing = 1

parallel_dag_run_count = 20
