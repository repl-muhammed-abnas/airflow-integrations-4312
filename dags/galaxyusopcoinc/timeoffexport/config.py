region = 'us-east-1'
environment = 'pre-production'

schedule = '0 1 * * *'
pacific_timezone = 'America/Los_Angeles'
pgp_conn_id = "pgp_vialto_partners"

aws_conn_id = "replicon.workato_S3_account"
bucket_name = 'replicon.integration_useast_s3_bucket'

execution_timeout_hours = 12
thread_pool_size_write_csv = 50
