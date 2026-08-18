from procore_ce_integration.productivity_unit_sync.config import *

instance = 'pmdemo3'
environment = 'pre-production'

aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_dag_id = f'procore_ce_productivity_unit_sync_webhook_{instance}'
main_dag_id = f'procore_ce_productivity_unit_sync_main_{instance}'
child_dag_id = f'procore_ce_productivity_unit_sync_child_{instance}'

productivity_unit_events_key = f"Procore_CE/{environment}/{instance}/webhooks/productivity_unit_events.json"

webhook_events_last_sync_time_var = f'procore_ce_productivity_unit_sync_last_sync_time_{instance}'
event_retention_days = 7

tenant_email = ['timmattlin@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

bearer_token_var = f'procore_ce_webhook_token_{instance}'
