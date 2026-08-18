# Environment Configuration
region = 'us-east-1'
environment = 'pre-production'

# Scheduling Configuration
schedule_interval = 30
schedule_interval_disable_master = '0 0 * * *'
time_zone = 'Etc/UTC'

# Performance Settings
max_active_runs = 1
execution_timeout_days = 14
file_sensor_timeout = 5
log_expiry = 7*24*60*60

# Child DAG max active runs
process_user_child_max_active_runs = 5
add_user_child_max_active_runs = 5
update_user_child_max_active_runs = 5
disable_user_child_max_active_runs = 5
disable_user_master_max_active_runs = 1
create_oef_tags_child_max_active_runs = 5
process_log_generation_child_max_active_runs = 1
supervisor_assignment_child_max_active_runs = 5
timeoff_with_logic_assignment_max_active_runs = 10

gather_logs_timeout_hours = 2
gather_user_details_timeout_hours = 4
trigger_process_user_record_child_parallel_count = 5

create_departments_child_max_active_runs = 5
create_locations_child_max_active_runs = 5
create_employeetypes_child_max_active_runs = 5
create_projectroles_child_max_active_runs = 5

user_details_report_name = 'User Details to Disable Report'
expected_report_columns = 'User Name,Employee ID,UserUri,Login Name,User Email,User End Date,User Status,Day Diff'

timeoffs_with_accrual_logic = ["Type1-A1", "Type1-A2", "Type1-B", "Type2-A", "Type2-B", "Type3"]
# These timeoffs will be manually added by Ipipeline HR team and shouldnt be updated/disabled by the integration
manually_added_timeoffs = ["Holiday Carry Over", "Holidays Bought"]

REP_DATE_FORMAT = "%m/%d/%Y"
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
YMD_DATE_FORMAT = "%Y/%m/%d"
REPLICON_DATE_FORMAT = "%Y-%m-%d"
