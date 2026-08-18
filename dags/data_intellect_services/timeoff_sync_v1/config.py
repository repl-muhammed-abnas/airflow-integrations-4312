region = 'eu-central-1'
environment = "pre-production"

sumo_conn_id = 'sumologic-dagrunlogger'

time_zone = "Europe/London"
schedule_interval = "0 */2 * * *"

max_active_runs = 1
max_active_booking_child = 5
execution_timeout_days = 14

can_run_batch_task_booking_child_var_name = "data_intellect_timeoff_sync_booking_child_can_run_batch_task"
can_run_batch_task_master_var_name = "data_intellect_timeoff_sync_master_can_run_batch_task"
