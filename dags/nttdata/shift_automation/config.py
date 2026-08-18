region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
existing_user_schedule_interval = "0 17 1 * *"
new_user_schedule_interval = "0 17 * * *"

execution_timeout_days = 14

shift_schedule_report_name = "**Shift Automation - Enabled User Report"

aws_conn_id = 'replicon.workato_S3_account'
aws_s3_bucket = 'replicon.integration_uswest_s3_bucket'
