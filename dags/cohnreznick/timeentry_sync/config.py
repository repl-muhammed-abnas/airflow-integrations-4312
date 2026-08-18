
region = "us-east-1"
environment = "pre-production"

user_base_report_name = "*** TimeEntry Sync User Base Report"
project_task_base_report_name = "*** TimeEntry Sync Project Base Report"

parallel_trigger_dagrun_count = 20


master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_hours = 10
execution_timeout_days = 14

master_max_active_run = 1
process_users_child_max_active_run = 3
max_active_run_log_generation = 1
process_each_timesheet_max_active_run = 3

#pylint: disable=line-too-long
expected_user_report_columns = "Login Name,Employee ID,user_uri,User Start Date,User End Date,User Status"
expected_project_report_columns = "project_uri,Project Name,Project Code,Project Status,Work Package/Work Item Code,Work Package/Work Item Name (Full Path),Work Package/Work Item Status,Task_uri,Work Package/Work Item Time & Expense Entry Type"
