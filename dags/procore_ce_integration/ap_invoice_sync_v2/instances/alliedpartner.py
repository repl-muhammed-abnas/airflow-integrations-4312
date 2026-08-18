# pylint: disable=wildcard-import
from procore_ce_integration.ap_invoice_sync_v2.config import *

instance = 'alliedpartner'
environment = 'production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_dag_id = f'procore_ce_ap_invoice_sync_v2_webhook_{instance}'
ap_invoice_main_dag_id = f'procore_ce_ap_invoice_sync_v2_main_{instance}'
ap_invoice_child_dag_id = f'procore_ce_ap_invoice_sync_v2_child_{instance}'

# Webhook configuration
bearer_token_var = f'procore_ce_webhook_token_{instance}'
webhook_events_last_sync_time_var = f'procore_ce_ap_invoice_sync_v2_last_sync_time_{instance}'

event_retention_days = 30
ap_invoice_events_key = f"Procore_CE/{environment}/{instance}/webhooks/v2/ap_invoice_events.json"
ap_invoice_failed_events_key = f"Procore_CE/{environment}/{instance}/webhooks/v2/ap_invoice_failed_events.json"

defer_origin_id_until_accepted = True

# Email configuration
tenant_email = ['mlawson@allied-fp.com']
internal_email = ['procoreintegrationsupport@deltek.com']
