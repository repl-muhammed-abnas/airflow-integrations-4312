region = 'us-east-1'
environment = 'pre-production'
company_key = 'AllenPhilpafmig'
user_data_report_name = 'User Report for Integration - RIT'
project_data_report_name = 'Project Report for Integration - RIT'
execution_timeout_days = 14
master_dag_interval = 30
max_active_runs_child = 5
max_active_runs_master = 1
max_active_runs_child_supplier = 10
max_active_runs_child_unassigned = 10

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_uswest_s3_bucket'
