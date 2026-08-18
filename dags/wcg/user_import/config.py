region = 'us-east-1'
environment = 'pre-production'

parallel_count = 10
schedule_interval_seconds = 30

max_active_runs = 1
process_user_child_max_active_runs = 5
add_user_child_max_active_runs = 5
update_user_child_max_active_runs = 5
process_log_generation_child_max_active_runs = 1
execution_timeout_days = 1

time_zone = 'America/New_York'
report_name = "Userlist"

NETSUITE_DATE_FORMAT = "%m/%d/%Y"
REP_DATE_FORMAT = "%m/%d/%Y"

oef_display_names = {
    "subsidiary": "User Subsidiary",
    "middle_name": "Middle Name",
}
