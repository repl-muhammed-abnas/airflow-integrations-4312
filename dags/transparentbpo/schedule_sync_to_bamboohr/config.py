environment = 'pre-production'
region = 'us-east-1'

schedule_interval = '0 23 * * *'
time_zone = 'America/Chicago'

shedule_report_name = "Enabled users - Schedule"
shift_report_name = "Enabled users - Shift assignment"

scheduled_user_expected_report_columns = "UserUri,User Name,Employee ID,Schedule Name (Current),Uniquevalue,Bamboo HR ID"
shift_user_expected_report_columns = "UserUri,User Name,Employee ID,Entry Date,Shift Name,DayDiff,Uniquevalue,Bamboo HR ID"

max_active_runs = 1
max_active_child_runs = 1
max_active_subchild_runs = 5

execution_timeout_days = 14
