"""
Unisys Resource Assignment - Configuration
Phase 2: File-based resource assignment to projects
"""

region = 'us-east-1'
environment = "pre-production"

# DAG Configuration
master_max_active_run = 1
max_active_runs_child = 10
execution_timeout_days = 7
parallel_count = 5

# Batch Configuration
ASSIGNMENT_BATCH_COUNT = 10

# Report Configuration
user_base_report_name = '***Active Users List***'
expected_user_report_columns = "Login Name,Employee ID,UserUri"

# File Processing
file_sensor_timeout = 10  # minutes

# Schedule
master_dag_interval = 30
time_zone = "Asia/Kolkata"

# Unisys-specific: Date format from Oracle
# Input: DD-MMM-YYYY (case insensitive)
# Examples: 01-JAN-2024, 01-jan-2024
DATE_FORMAT_INPUT = "%d-%b-%Y"
DATE_FORMAT_OUTPUT = "%Y-%m-%d"

# Airflow Variables
can_decrypt_file_var_name = 'unisys_resource_assignment_can_decrypt_file'
can_run_batch_task_var_name = 'unisys_resource_assignment_batch_task_enabled'
