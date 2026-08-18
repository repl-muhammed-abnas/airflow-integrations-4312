region = "us-east-1"
environment = "pre-production"

max_active_runs = 1
schedule_interval = "0 15 * * 1"
time_zone = "America/New_York"

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

execution_timeout_mins_write_csv = 90

export_report_name="GL Weekly Activity Export Entry Date"
export_approval_report_name="GL Weekly Activity Export Approval Date"
# Batch task variable for tracking batch operations
batch_task_variable = "mercury_systems_inc_gl_project_time_batch_task_variable"
expected_report_columns="Full Name,Employee ID,Chargeable Flag,Labor Classification,Job Code,Pay Type,Timesheet End Date,Business Unit,ADP Department,Activity,Weekly Hours,Weekly Earnings,Time In"