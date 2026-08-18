region = 'eu-central-1'
environment = 'pre-production'

location = 'India'
max_active_runs = 1
max_active_approved_runs = 10
parallel_runs_approved_bookings = 20
parallel_runs_deleted_bookings = 20
time_zone = "UTC"
schedule_interval = '0 1 * * *'

dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

approved_timeoffs_report = 'GTM-INT-008 Payroll HGS Integration - Approved'
deleted_timeoffs_report = 'GTM-INT-008 Payroll HGS Integration - Deleted'
# pylint: disable=line-too-long
expected_approved_timeoffs_report_columns = 'Leave Request ID,Local Employee Number,Employee ID,Time Off Type,Time Off Type Description,Booking Start Date,Booking End Date,Approval Status,Cost Center (Current) (Full Path)'
expected_deleted_timeoffs_report_columns = 'Leave Request ID,Local Employee Number,Employee ID,Current Time Off Type,Current Start Date,Current End Date,Action,Cost Center (Current) (Full Path)'

execution_timeout_mins_write_csv = 90
execution_timeout_days = 14
gather_logs_timeout_hours = 12

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'
