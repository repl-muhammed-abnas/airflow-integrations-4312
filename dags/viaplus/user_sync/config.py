"""
ViaPlus User Sync Configuration

Global configuration and constants for ViaPlus Keka HR to Replicon User Sync Integration.
Sync happens only for Indian employees where:
- Legal Entity: VPTI Solutions Private Limited
- Exclude Department: General and Administration
"""
# Region and Environment
region = "us-east-1"
environment = "pre-production"

# Execution timeouts
execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

# Max active runs for DAGs
max_active_runs_master = 1
max_active_runs_process_users = 10
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10
max_active_runs_process_supervisor = 10
max_active_runs_process_log_generation = 1
disable_user_child_dag_active_runs = 5
disable_user_master_dag_active_runs = 1

# Batch and parallel processing
BATCH_COUNT = 2
trigger_parallel_dagrun_count_process_users = 10

# Disable future end date user DAG config
indian_timezone = 'Asia/Kolkata'
report_name = '***Disable User Template - For User Import'
disable_user_master_dag_interval = '0 1 * * *'

# Keka API Configuration
KEKA_GRANT_TYPE = "kekaapi"
KEKA_SCOPE = "kekaapi"

# Filter criteria for ViaPlus India
LEGAL_ENTITY_NAME = "VPTI Solutions Private Limited"
EXCLUDED_DEPARTMENTS = ["General and Administration"]

# Date format
DATE_FORMAT = "%d/%m/%Y"  # DD/MM/YYYY

# Replicon Permission Sets
DEFAULT_PERMISSION = "Project Resource with Reports"
SUPERVISOR_PERMISSION = "Supervisor"
REPORT_USER_PERMISSION = "Project Resource with Reports"

# Timesheet Configuration
TIMESHEET_TEMPLATE = "Timesheet - ViaPlus"
TIMESHEET_APPROVAL_PATH = "Supervisor"
TIMESHEET_PERIOD = "Weekly starting on Monday"

# Time-Off Configuration
TIME_OFF_TEMPLATE = "Time Off"
APPLICABLE_TIME_OFF_TYPES = [
    "Comp Offs",
    "Paid Leave",
    "Unpaid Leave",
    "Sick Leave",
    "Maternity Leave",
    "Paternity Leave",
    "Adoption Leave",
    "Holiday",
    "Floater Leave"
]

# Schedule Configuration
TIMEZONE = "(UTC+5:30) India Standard Time"
WORK_WEEK_START = "urn:replicon:day-of-week:monday"
HOLIDAY_CALENDAR = {
    "Hyderabad": "India - Hyderabad",
    "Bangalore": "India - Bangalore"
}
OFFICE_SCHEDULE = "8 hours/day; Mon-Fri"

# Employee Type (hardcoded for all)
DEFAULT_EMPLOYEE_TYPE = "Regular Employee"

# License Configuration
LICENSES_TO_ASSIGN = ["Polaris PSA", "TimeOff Enterprise"]

initial_sync_time = '2025-12-29T00:00:00+00:00'
time_format = '%Y-%m-%dT%H:%M:%S+00:00'