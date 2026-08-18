"""
Guidehouse Resource Assignment - Configuration
Phase 2: File-based resource assignment to projects
"""

region = 'us-east-1'
environment = "pre-production"

# DAG Configuration
master_max_active_run = 1
max_active_runs_child = 10
execution_timeout_days = 7
parallel_count = 10

# Batch Configuration
ASSIGNMENT_BATCH_COUNT = 10

# Report Configuration
user_base_report_name = '***Active Users List***'
expected_user_report_columns = "Login Name,Employee ID,UserUri"

# File Processing
file_sensor_timeout = 10  # minutes

# Completion log file download link validity (7 days)
log_file_download_link_expiry_in_sec = 7 * 24 * 60 * 60

# Project-import files share this integration's inbound SFTP folder. Used to recognise
# and SKIP (not archive) PeopleSoft project-import files: <project_file_prefix><digits>.txt.pgp
project_file_prefix = "PPS_Project_"

# Schedule
master_dag_interval = 30
time_zone = "Asia/Kolkata"

# Guidehouse-specific: Date format from Peoplesoft
# Input: YYYY-MM-DD
# Examples: 2024-01-01, 2026-12-31
DATE_FORMAT_INPUT = "%Y-%m-%d"
DATE_FORMAT_OUTPUT = "%Y-%m-%d"

# Airflow Variables
can_decrypt_file_var_name = 'guidehouse_resource_assignment_can_decrypt_file'
can_run_batch_task_var_name = 'guidehouse_resource_assignment_batch_task_enabled'
