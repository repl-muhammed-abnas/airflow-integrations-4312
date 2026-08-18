region = 'us-east-1'
environment = 'pre-production'

report_name = 'User list for Integration'
expected_report_columns = 'UserUri,Permission Name,Date Account Last Changed,Description,User Name'

execution_timeout_days = 14
max_active_runs = 1
max_child_active_runs = 1
schedule_interval = '0 20 * * *'
time_zone = 'America/Los_Angeles'
download_link_expiry = 7*24*60*60

aws_conn_id = "replicon.workato_S3_account"
bucket_name = 'replicon.integration_uswest_s3_bucket'
