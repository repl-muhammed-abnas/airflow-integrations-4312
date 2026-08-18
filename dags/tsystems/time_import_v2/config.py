"""Configuration settings for T-Systems Time Import integration."""

region = "eu-central-1"
environment = "pre-production"

# Timezone Configuration
timezone = 'Etc/UTC'

# Processing Configuration
process_parallel_count = 2 # Number of parallel child DAGs
max_active_runs_master = 1
max_active_runs_child = 4
max_active_runs_log_gen_child = 1
csv_separator = ';'
execution_timeout_days = 14
file_sensor_timeout = 5  # Timeout in minutes

# Adds a 'Username' column to the log report, alongside Employee ID.
# Enabled per-instance in instances/*.py - currently trial only.
include_username_in_logs = False

# Column Mapping
column_mapping = {
    'Reported by': 'reported_by',
    'Employee ID': 'employee_id', 
    'Entry Date': 'entry_date',
    'In time': 'in_time',
    'Out time': 'out_time',
    'WorkType': 'work_type',
    'Project ID': 'project_id',
    'Task Name': 'task_name',
    'Billing rate name': 'billing_rate_name',
    'Activity': 'activity',
    'Hours': 'hours',
    'Comments': 'comments'
}

time_import_eligibility_oef_name = "Eligible for Time Import"

# Validation Configuration
mandatory_fields = ['employee_id', 'entry_date']
entry_dateformat = '%d/%m/%Y'
time_format = '%H:%M'

# User OEF
worktype = 'WorkType HR200'
worktype_tarif = 'WorkType HR200 Tarif'
worktype_tariffrei = 'WorkType HR200 Tariffrei'

# Maps timesheet template name to its assigned WorkType OEF
# Used to resolve the correct OEF when the same work type value exists in multiple OEFs
template_worktype_oef_mapper = {
    "Internal Worktype": worktype,
    "HR200 integr. RZ": worktype,
    "HR200 FZ=AZ": worktype,
    "HR200 tariffrei": worktype_tariffrei,
    "HR200 Tarif": worktype_tarif,
}

# Timesheet Types
timesheet_dist = 'Time Distribution only'
timesheet_inout_dist = 'In/Out plus Time Distribution'
timesheet_inout_dist_with_oef = 'In/Out with Time entry against Custom field plus Time distribution'