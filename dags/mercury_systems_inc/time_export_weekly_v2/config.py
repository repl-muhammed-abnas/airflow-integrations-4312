region = "us-east-1"
environment = "pre-production"

max_active_runs = 1
schedule_interval = "0 4 * * 1"  # Run every Monday at 4 AM
time_zone = "America/New_York"

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

execution_timeout_mins_write_csv = 90

sftp_export_file_path = "/Outbound/GLLaborCostExport/Input"
sftp_export_archive_path = "/Outbound/GLLaborCostExport/Archive"
# Batch task variable for tracking batch operations
batch_task_variable = "mercury_systems_inc_time_export_batch_task_variable"
user_report_name="UserDetailsForExport"
user_report_columns = "Employee ID,User First Name,User Last Name"
# File format for time export
time_export_file_format = 'weekly_time_export'

# Time export settings
can_send_time_export_downstream = "mercury_systems_inc_time_export_send_downstream"
thread_pool_size=5

