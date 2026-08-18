region = "us-east-1"
environment = "pre-production"

# Common configuration parameters
parallel_trigger_dagrun_count = 5
file_sensor_timeout = 10
execution_timeout_days = 14
time_zone = "America/New_York"

# Max active runs
master_max_active_run = 1
process_child_max_active_run = 5
max_active_run_log_generation = 1

# Expected report columns
expected_csv_columns = "Project / WO Code,Task / Operation Code,ProjectUri,TaskUri,Department (Full Path),Employee ID"
project_task_report_name = "ProjectAndTaskForTimeImport"
# For batch processing
expected_user_report_columns = "UserUri,TimeSheetTemplate,Employee ID,User Status,Department (Current) (Full Path)"
user_report_name = "UserDetailsForTimeImport"

batch_task_variable = "mercury_systems_inc_time_import_batch_task_variable"
