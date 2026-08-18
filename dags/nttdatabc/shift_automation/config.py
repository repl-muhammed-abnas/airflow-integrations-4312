region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1

time_zone = "America/Los_Angeles"

existing_user_schedule_interval = "0 1 L * *"
new_user_schedule_interval = "0 1 1 * *"

execution_timeout_days = 14

shift_schedule_report_name = "**Shift Automation - Enabled User Report"

aws_conn_id = 'replicon.workato_S3_account'
aws_s3_bucket = 'replicon.integration_uswest_s3_bucket'
