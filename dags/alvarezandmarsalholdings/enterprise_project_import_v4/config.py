environment = 'pre-production'
region = 'us-east-1'

max_active_runs_master = 1

execution_timeout_days = 14
gather_project_logs_timeout_hours = 2

trigger_parallel_dagrun_count_process_projects = 6
trigger_parallel_dagrun_count_process_resource = 6

max_active_runs_process_projects = 5
max_active_runs_process_resource = 5
max_active_runs_process_log_generation = 1

# Project OEF
PROJECT_PROFILE = "Project Profile"
CONTROLLING_AREA = "Controlling Area"

# Task OEF
BILLING_RESPONSIBLE = "Billing Responsible"
BILLING_CONTROL_CATEGORY = 'Billing Control Category'

# Permissions
PERMISSIONS = [
    "Project Manager",
    "End User with Report Edit",
    "Supervisor"
]

dagrun_log_conn_id = 'sumologic-dagrunlogger'

# Resource assignment: max resource URIs per BulkUpdateResourceAssignments payload.
# Override per instance to control per-call API load.
resource_batch_size = 200

ADD_RESOURCE_BATCH_COUNT = 4

