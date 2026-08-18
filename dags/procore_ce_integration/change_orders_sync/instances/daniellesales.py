# pylint: disable=wildcard-import, unused-import
from procore_ce_integration.change_orders_sync.config import *

instance = 'daniellesales'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_dag_id = f'procore_computerease_budget_revision_webhook_{instance}'
main_dag_id = f'procore_computerease_budget_revision_sync_main_{instance}'
child_dag_id = f'procore_computerease_budget_revision_sync_child_{instance}'
bulk_sync_dag_id = f'procore_computerease_budget_revision_bulk_sync_{instance}'

bearer_token_var = f'procore_ce_webhook_token_{instance}'
webhook_events_last_sync_time_var = f'procore_ce_budget_revision_last_sync_time_{instance}'
budget_revision_events_key = f"Procore_CE/{environment}/{instance}/webhooks/budget_revision_events.json"

tenant_email = ['DanielleMottl@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
