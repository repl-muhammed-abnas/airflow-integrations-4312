region = 'us-east-1'
instance = "trial"
environment = 'pre-production'

time_zone = "Etc/UTC"
log_file_link_expiry = 7*24*60*60
master_dag_active_runs = 1
child_dag_max_active_runs = 1
create_user_child_max_active_runs = 5
update_user_child_max_active_runs = 5
process_user_child_max_active_runs = 5
create_groups_child_max_active_runs = 1
create_oef_tags_child_max_active_runs = 3
enable_user_child_max_active_runs = 5
disable_user_child_max_active_runs = 5
change_user_status_master_max_active_runs = 1
supervisor_assignment_child_max_active_runs = 5
trigger_parallel_dagrun_count = 10
supervisor_assignment_parallel_count = 5
execution_timeout_days = 14
provider = 'bamboohr'
workflow = 'user_import'
schedule_interval = "0 */12 * * *"
schedule_interval_change_status_master = "0 1 * * *"
user_permission_set = ["Project Resource with Reports"]
supervisor_permission_set = ["Supervisor", "Project Resource with Reports"]
all_license_types = ["Workforce Management", "Expense Plus", "TimeBill Plus", "TimeOff Enterprise", "ZeroTime"]
licenses = ["Workforce Management", "Expense Plus", "TimeBill Plus", "TimeOff Enterprise"]
all_notifications = ['expense-sheet', 'holiday', 'pay-rule-script', 'project',
    'time-entry-revision-group', 'time-off', 'time-punch-action', 'timesheet', 'user']

user_details_report_name = 'User Details Status Update Report'
expected_report_columns = "User Name,Employee ID,UserUri,User Start Date,User End Date,User Status,Start Day Diff,End Day Diff"

STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
BAMBOOHR_LASTCHANGED_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
