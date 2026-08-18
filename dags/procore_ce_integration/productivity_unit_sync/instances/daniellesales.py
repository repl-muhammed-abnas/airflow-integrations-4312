from procore_ce_integration.productivity_unit_sync.config import *

instance = 'daniellesales'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

webhook_dag_id = f'procore_ce_productivity_unit_sync_webhook_{instance}'
main_dag_id = f'procore_ce_productivity_unit_sync_main_{instance}'
child_dag_id = f'procore_ce_productivity_unit_sync_child_{instance}'

productivity_unit_events_key = f"Procore_CE/{environment}/{instance}/webhooks/productivity_unit_events.json"

webhook_events_last_sync_time_var = f'procore_ce_productivity_unit_sync_last_sync_time_{instance}'

tenant_email = ['DanielleMottl@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

bearer_token_var = f'procore_ce_webhook_token_{instance}'
