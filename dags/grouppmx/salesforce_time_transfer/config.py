region = 'us-east-1'
environment = 'pre-production'
timezone = 'EST'
schedule_interval = '0 2 * * *'

time_entry_report_name = 'Salesforce Time Sync'
log_filepath = 'timetransfer_logs'

expected_time_entry_report_columns = 'Client Name,Project Name,User Name,Entry Date,Hours Worked,Time Off Hrs,ClientUri,ProjectUri,UserUri,TimesheetPeriodUri,Timesheet Start Date,Timesheet End Date,Billing Rate Name,Billing Rate Amount,Activity Name,User Supervisor Name (Current),Actual Billable Hours (Selected Dates),Actual Non-Billable Hours (Selected Dates),Time Off Type,Approval Status,Submitted On,Approver Name,Approval Date/Time'

execution_timeout_days = 14

child_dag_max_active_runs = 5
master_dag_max_active_runs = 1
