# pylint: disable=wildcard-import
from procore_ce_integration.ar_invoice_sync.config import *
from procore_ce_integration.job_structure_sync.instances.trial import default_wbs_type

instance = 'pmsid'
environment = 'pre-production'

# Connection IDs for trial environment
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

ar_invoice_main_dag_id = f'procore_ce_ar_invoice_sync_main_{instance}'
ar_invoice_child_dag_id = f'procore_ce_ar_invoice_sync_child_{instance}'
child_dag_max_active_runs = 5

# Email configuration
tenant_email = ['SiddhantrajSingh@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

# Webhook configuration
bearer_token_var = f'procore_ce_webhook_token_{instance}'
ar_invoice_events_key = f"Procore_CE/{environment}/{instance}/webhooks/ar_invoice_events.json"
