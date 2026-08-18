region = 'eu-central-1'
environment = "pre-production"
time_zone = "EST"
master_max_active_runs = 1
max_active_parallel_runs = 5
max_active_child_runs = 5
schedule_interval = "30 13 * * *"
execution_timeout_days = 14
sumo_conn_id = "sumologic-dagrunlogger"
submit_time_child_max_active_runs = 5
shiftassignment_export_last_run_time = "wipro_shiftassignment_export_last_run_time"
active_user_report_name = "RIT_User_Shift_Assignment_Report"
expected_report_columns = "Employee ID,Entry Date,User Name,Country (Current),UserUri,User Status,shifturi,Shift Name,Shift Start Time,Shift End Time"

#in multiple of 7 so that we get all 7 days data in one batch for a user who is at the end of batch
batch_size = 504
