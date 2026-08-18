region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
max_active_runs = 1
child_max_active_runs = 5

# Batch DAG configuration
# DEFAULT_MAX_BATCH_COUNT: Number of child DAGs to create (controls concurrency)
#                          Countries are distributed across these DAGs
#                          With child_max_active_runs=5, this controls max parallel API calls
DEFAULT_MAX_BATCH_COUNT = 5

time_zone = "Etc/UTC"

execution_timeout_mins_write_csv = 90

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

report_name = "GTM INT007 FSD LeaveHeader(ISG DB) - ALL"

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Leave Carry Forward;Leave Accrued;Leave Availed;Leave Reset;Leave Balance;Units;Pushed On;User End Date"

export_columns = [
    'Employee ID', 'Local Employee Number', 'Time Off Type', 'Time Off Type Description',
    'Leave Carry Forward', 'Leave Accrued', 'Leave Availed', 'Leave Reset', 'Leave Balance',
    'Units', 'Pushed On', 'User End Date'
]

leave_status = 'leave balance'
