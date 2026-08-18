from procore_ce_integration.productivity_unit_sync.config import *

instance = 'bluecord'
environment = 'production'

# Connection IDs
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
webhook_dag_id = f'procore_ce_productivity_unit_sync_webhook_{instance}'
main_dag_id = f'procore_ce_productivity_unit_sync_main_{instance}'
child_dag_id = f'procore_ce_productivity_unit_sync_child_{instance}'

productivity_unit_events_key = f"Procore_CE/{environment}/{instance}/webhooks/productivity_unit_events.json"

# Event tracking
webhook_events_last_sync_time_var = f'procore_ce_productivity_unit_sync_last_sync_time_{instance}'
event_retention_days = 30

# Email configuration
tenant_email = ['mrose@blue-cord.com']
internal_email = ['procoreintegrationsupport@deltek.com']

# Webhook configuration
bearer_token_var = f'procore_ce_webhook_token_{instance}'

disabled = True
