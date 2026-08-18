# pylint: disable=wildcard-import, unused-import
from procore_ce_integration.subcontract_change_order_sync.config import *

instance = 'monarch'
environment = 'production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_processing_dag_id = f'procore_computerease_change_order_webhook_processing_{instance}'
main_dag_id = f'procore_computerease_change_order_sync_main_{instance}'
project_child_dag_id = f'procore_computerease_change_order_sync_project_child_{instance}'
cop_child_dag_id = f'procore_computerease_change_order_sync_cop_child_{instance}'
attachment_child_dag_id = f'procore_ce_attachment_upload_child_dag_{instance}'


change_order_events_key = f"Procore_CE/{environment}/{instance}/webhooks/change_order_events.json"
webhook_events_last_sync_time_var = f'procore_ce_change_order_last_sync_time_{instance}'
bearer_token_var = f'procore_ce_webhook_token_{instance}'

tenant_email = ['tina.dodson@monarch.build', 'kristin.tucker@monarch.build']
internal_email = ['procoreintegrationsupport@deltek.com']

event_retention_days = 30
