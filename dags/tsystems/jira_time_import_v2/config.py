region = "eu-central-1"
environment = "pre-production"

# Processing Configuration
process_parallel_count = 2
max_active_runs_master = 1
max_active_runs_child = 4
max_active_runs_log_gen_child = 1
send_email_max_active_runs_child = 10
csv_separator = ';'
execution_timeout_days = 14
file_sensor_timeout = 5

# Column Mapping
column_mapping = {
    "ID_Country": "country_id",
    "ID_System": "system_id",
    "ID": "unique_id",
    "Login": "employee_id",
    "Work_Date": "entry_date",
    "Work_Hours": "hours",
    "WBS_Code": "project_id",
    "Task_Name": "full_task_path",
    "Comments": "comments"
}

project_details_report = "Project Details - Jira Time Import"
user_details_report = "User Details - Jira Time Import"

activity_name = "Working time"

# Validation Configuration
ENTRY_DATE_FORMAT = '%d/%m/%Y'
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
