# pylint: disable=wildcard-import
from procore_ce_integration.purchase_order_sync.config import *

instance = 'sushmamathi2'
environment = 'pre-production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'procore_computerease_purchase_order_sync_main_{instance}'
child_dag_id = f'procore_computerease_purchase_order_sync_child_{instance}'
webhook_dag_id = f'procore_computerease_purchase_order_sync_webhook_{instance}'

# Email configuration
tenant_email = ['sushmamathi@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

# Webhook configuration
bearer_token_var = f'procore_ce_webhook_token_{instance}'


# Last sync time tracking
webhook_events_last_sync_time_var = f'procore_ce_purchase_order_sync_last_sync_time_{instance}'
purchase_order_events_key = f"Procore_CE/{environment}/{instance}/webhooks/purchase_order_events.json"
