region = 'us-east-1'
environment = 'pre-production'
company_key = 'WolverinePipelineafmig'
time_zone = "America/New_York"
execution_timeout_days = 14
max_active_runs_child = 1
max_active_runs_master = 1
report_name = "Paychex Report for Replicon Integration"

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon-integrations-uswest_s3_bucket'
