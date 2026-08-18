region = 'us-east-1'
environment = "pre-production"

# DAG Execution Settings
master_max_active_run = 1
max_active_runs_child = 10
execution_timeout_days = 14
parallel_count = 10

# Batch Processing Configuration
PROJECT_BATCH_COUNT = 10

# File Processing
file_sensor_timeout = 10  # minutes
master_dag_interval = 30
time_zone = "Asia/Kolkata"

# Business Logic Constants (from tech spec)
DEFAULT_PAYCODE = "105"
DEFAULT_TIME_TYPE_VALIDATION = "Client"
PROJECT_STATUS_ACTIVE = "Active"
PROJECT_STATUS_CLOSED = "Closed"

# Date format from Oracle (tech spec: DD-MMM-YYYY, case insensitive)
DATE_FORMAT_INPUT = "%d-%b-%Y"
PROJECT_MANAGER_REQUIRED_PERMISSIONS = [
    "Project Manager",
    "Resource Manager (for PM)"
]
