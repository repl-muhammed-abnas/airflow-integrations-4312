# pylint: disable=wildcard-import, unused-import
from procore_ce_integration.change_orders_sync.config import *

instance = 'saybrconstruction'
environment = 'production'

event_retention_days = 30  # for production

aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_dag_id = f'procore_computerease_budget_revision_webhook_{instance}'
main_dag_id = f'procore_computerease_budget_revision_sync_main_{instance}'
child_dag_id = f'procore_computerease_budget_revision_sync_child_{instance}'
bulk_sync_dag_id = f'procore_computerease_budget_revision_bulk_sync_{instance}'

budget_revision_events_key = f"Procore_CE/{environment}/{instance}/webhooks/budget_revision_events.json"

bearer_token_var = f'procore_ce_webhook_token_{instance}'
webhook_events_last_sync_time_var = f'procore_ce_budget_revision_last_sync_time_{instance}'

tenant_email = ['bsay@saybr.com']
internal_email = ['MPTeamReplicon@deltek.com', 'procoreintegrationsupport@deltek.com']
