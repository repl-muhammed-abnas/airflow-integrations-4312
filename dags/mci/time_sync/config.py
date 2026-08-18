region = 'us-east-1'
environment = 'pre-production'

max_active_runs_master = 1
execution_timeout_days = 14
child_dag_max_active_runs = 4
gather_time_sync_logs_timeout_hours = 2
max_active_runs_process_log_generation = 1

timeoff_report_name = 'Approved Timesheet data for integration'

expected_report_columns = 'User Name,Employee Type (Current) (Full Path),Employee ID,Entry Date,Time In,Time Out,Total Hrs,Timesheet Period,Timesheet Start Date,Timesheet End Date,Time Off Hrs,Time Off Type'
