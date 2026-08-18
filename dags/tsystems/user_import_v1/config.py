# Region and environment
region = 'eu-central-1'
environment = 'pre-production'

parallel_count = 10
schedule_interval = '0 */1 * * *'
schedule_interval_disable_master = '0 0 * * *'
execution_timeout_hours = 4
task_timeout_minutes = 30
log_file_link_expiry = 7*24*60*60
csv_separator = ';'

# Performance settings for different processes
max_active_runs = 1
process_user_child_max_active_runs = 5
add_user_child_max_active_runs = 5
update_user_child_max_active_runs = 5
create_holiday_calendar_child_max_active_runs = 5
process_user_details_from_api_child_max_active_runs = 5
create_oef_tags_child_max_active_runs = 5
process_log_generation_child_max_active_runs = 1
disable_user_child_max_active_runs = 5
disable_user_master_max_active_runs = 1
supervisor_assignment_child_max_active_runs = 5
execution_timeout_days = 1
gather_logs_timeout_hours = 4
gather_user_details_timeout_hours = 4

PROCESS_USER_BATCH_COUNT = 10
trigger_parallel_dagrun_count_process_users = 10
trigger_parallel_dagrun_count_process_supervisors = 10


# Variable names for controlling batch tasks
can_run_batch_task_var_name = 'tsystems_can_run_batch_task_sandbox'

# Time zone settings
time_zone = 'Etc/UTC'

STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
YMD_DATE_FORMAT = "%Y/%m/%d"
REP_DATE_FORMAT = "%d.%m.%Y"

user_details_report_name = 'User Details to Disable Report'
expected_report_columns = "User Name;Employee ID;UserUri;User End Date;User Status;Day Diff"
