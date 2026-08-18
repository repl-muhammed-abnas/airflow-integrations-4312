# pylint: disable=wildcard-import
from procore_ce_integration.job_structure_sync.config import *

instance = 'ascc'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

event_retention_days = 30  # for production


# DAG IDs
webhook_processing_dag_id = f'procore_ce_jobs_webhook_processing_dag_{instance}'
job_structure_main_dag_id = f'procore_ce_job_structure_sync_main_dag_{instance}'
job_structure_child_dag_id = f'procore_ce_job_structure_sync_child_dag_{instance}'
budget_line_item_dag_id = f'procore_ce_job_structure_get_budget_line_item_dag_{instance}'
prime_contract_line_items_dag_id = f'procore_ce_prime_contract_line_items_dag_{instance}'

job_structure_events_key = f"Procore_CE/{environment}/{instance}/webhooks/job_structure_events.json"

webhook_events_last_sync_time_var = f'procore_ce_job_sync_last_sync_time_{instance}'
bearer_token_var = f'procore_ce_webhook_token_{instance}'

tenant_email = ['thomasji@asccinc.com', 'workmanbs@asccinc.com', 'kazilrs@asccinc.com']
internal_email = ['procoreintegrationsupport@deltek.com']
