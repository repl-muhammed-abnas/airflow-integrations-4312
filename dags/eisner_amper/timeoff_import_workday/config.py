region = "us-east-1"
environment = "pre-production"

# Processing Configuration
process_parallel_count = 10
max_active_runs_master = 1
max_active_runs_process_users_child = 5
max_active_runs_process_time_entries_child = 5
max_active_runs_log_gen_child = 1
csv_separator = ','
execution_timeout_days = 14
file_sensor_timeout = 5

# Column Mapping
column_mapping = {
    "employeeid": "employee_id",
    "Start Time": "start_time",
    "Start Date": "start_date",
    "End Time": "end_time",
    "End Date": "end_date",
    "Time Off Unit": "hours",
    "Time Off Type Project Code": "project_code",
    "Booking Reference ID": "booking_reference_id"
}

# Validation Configuration
ENTRY_DATE_FORMAT = '%m/%d/%Y'
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

DEFAULT_TASK_NAME = "Default Task"
WORK_LOCATION_OEF_NAME = "Work Location"
