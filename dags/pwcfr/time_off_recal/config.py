region = 'eu-central-1'
environment = 'pre-production'
company_key = 'pwcfrafmig'
time_zone = "Europe/Paris"
report_name = 'Timeoff Recal Automation'
execution_timeout_days = 14
max_active_runs_child = 5
max_active_runs_master = 1
batchsize = 200

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'
