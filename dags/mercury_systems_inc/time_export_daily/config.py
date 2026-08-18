region = "us-east-1"
environment = "pre-production"

max_active_runs = 1
schedule_interval = "0 23 * * *"
time_zone = "America/New_York"

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

execution_timeout_mins_write_csv = 90

sftp_base_path = "/Outbound/TimeExport"
export_report_name="Time Data Export to Oracle"
# Batch task variable for tracking batch operations
batch_task_variable = "mercury_systems_inc_time_export_batch_task_variable"
expected_report_columns="Employee ID,Project Name,Project Code,Task Name,Task Code,Entry Date,Hours,Employee Approval,Manager Approval,Operating Unit,Chargeable Flag,User First Name,User Last Name,ADP Department,Charge Type Name,Approval Day Diff,Time Entry ID"
# Time export settings
export_file_prefix = "MercurySystems"