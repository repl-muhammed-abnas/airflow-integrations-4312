region = "us-east-1"
environment = "pre-production"

team_id = "CIE"


ts_report_name = "TS_TO_autoapproval"
to_report_name = "TimeOff Booking Details"

timesheet_approve_remarks = "Timesheet Approved by CIE Approval Utility."

max_child_run = 3
max_submit_child_run = 3
execution_timeout_days = 14
chunk_size = 100
submit_chunk_size = 100

date_format = "%m/%d/%Y"
report_date_format = '%b %d, %Y'


not_submitted_filter_value = "0"
waiting_for_approval_filter_value = "1"
location = ""
can_run_batch_task_var_name = 'true'
error_severity = "urn:replicon:severity:error"