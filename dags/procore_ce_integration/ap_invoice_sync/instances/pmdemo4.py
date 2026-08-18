# pylint: disable=wildcard-import
from procore_ce_integration.ap_invoice_sync.config import *
instance = 'pmdemo4'
environment = 'pre-production'

# Connection IDs for pre-production environment
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

ap_invoice_main_dag_id = f'procore_ce_ap_invoice_sync_main_{instance}'
ap_invoice_child_dag_id = f'procore_ce_ap_invoice_sync_child_{instance}'

# Webhook configuration
bearer_token_var = f'procore_ce_webhook_token_{instance}'
failed_invoices_var = f'procore_ce_ap_invoice_sync_failures_{instance}'


ap_invoice_events_key = f"Procore_CE/{environment}/{instance}/webhooks/ap_invoice_events.json"

# Email configuration
tenant_email = ['christinehill@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
