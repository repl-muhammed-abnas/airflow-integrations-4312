# pylint: disable=wildcard-import
from procore_ce_integration.subcontract_change_order_sync.config import *

instance = 'HensonConstruction'
environment = 'production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_processing_dag_id = f'procore_computerease_change_order_webhook_processing_{instance}'
main_dag_id = f'procore_computerease_change_order_sync_main_{instance}'
project_child_dag_id = f'procore_computerease_change_order_sync_project_child_{instance}'
cop_child_dag_id = f'procore_computerease_change_order_sync_cop_child_{instance}'
attachment_child_dag_id = f'procore_ce_attachment_upload_child_dag_{instance}'


webhook_events_last_sync_time_var = f'procore_ce_change_order_last_sync_time_{instance}'
change_order_events_key = f"Procore_CE/{environment}/{instance}/webhooks/change_order_events.json"
bearer_token_var = f'procore_ce_webhook_token_{instance}'

event_retention_days = 30

sync_prime_contract_change_order = False # False if change order sync is enabled
sync_commitment_contract_change_order = True

tenant_email = ['mstine@hensonco.biz']
internal_email = ['procoreintegrationsupport@deltek.com']
