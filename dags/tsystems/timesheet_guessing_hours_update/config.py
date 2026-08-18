region = "eu-central-1"
environment = "pre-production"
company_key = "TsystemsSB"

report_name = "Time Entry Details - Guessing Hours"
expected_report_columns = "Employee ID;User Name;User URI;Entry Date;Hours;Project Name;Task Name;Task URI;Org Structure Code (Current);Org Structure (Current) (Full Path);Timesheet Start Date;Entry ID"

schedule = "30 0 * * *"
time_zone = "Etc/UTC"

# Processing Configuration
max_active_runs_master = 1
max_active_runs_child = 5
max_active_runs_log_gen_child = 1
csv_separator = ';'
execution_timeout_days = 14
file_sensor_timeout = 5

trigger_process_entries_parallel_dagrun_count = 10
trigger_process_users_parallel_dagrun_count = 10

# Validation Configuration
ENTRY_DATE_FORMAT = '%d/%m/%Y'
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

MAPPER_DATE_FORMAT = "%d.%m.%Y"
REPORT_DATE_FORMAT = "%m/%d/%Y"