region = 'us-east-1'
environment = 'pre-production'

aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

# DAG execution settings
execution_timeout_days = 7
webhook_dag_max_active_runs = 1
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5

# Scheduling
schedule_in_seconds = 300

# Date formats
ce_date_format = '%m/%d/%Y' # MM/DD/YYYY format
procore_api_date_format = '%Y-%m-%dT%H:%M:%SZ'
procore_webhook_date_format = '%Y-%m-%dT%H:%M:%S.%fZ'

# Event Cleanup
event_retention_days = 7
event_clean_interval_hours = 24
is_paused_upon_creation = True
internal_email = ['procoreintegrationsupport@deltek.com']
