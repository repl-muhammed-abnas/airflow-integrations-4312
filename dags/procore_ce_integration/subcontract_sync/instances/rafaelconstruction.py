# pylint: disable=wildcard-import
from procore_ce_integration.subcontract_sync.config import *

instance = 'rafaelconstruction'
environment = 'production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

event_retention_days = 30

# DAG IDs
webhook_processing_dag_id = f'procore_ce_subcontract_webhook_processing_dag_{instance}'
subcontract_main_dag_id = f'procore_ce_subcontract_sync_main_dag_{instance}'
subcontract_child_dag_id = f'procore_ce_subcontract_sync_child_dag_{instance}'
attachment_child_dag_id = f'procore_ce_attachment_upload_child_dag_{instance}'

subcontract_events_key = f"Procore_CE/{environment}/{instance}/webhooks/subcontract_events.json"

webhook_events_last_sync_time_var = f'procore_ce_subcontract_sync_last_sync_time_{instance}'
bearer_token_var = f'procore_ce_webhook_token_{instance}'

tenant_email = ['Nichole@RafaelCompanies.com']
internal_email = ['procoreintegrationsupport@deltek.com']
