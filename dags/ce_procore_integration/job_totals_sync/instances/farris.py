# pylint: disable=wildcard-import,unused-import
from ce_procore_integration.job_totals_sync.config import *
from ce_procore_integration.util_dags.instances.farris import wbs_code_creator_dag_id

instance = 'farris'
region = 'us-east-1'
environment = 'pre-production'

schedule = '0 */2 * * *'
retry_delays_hours = [0, 24, 48, 72, 168]

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

job_totals_main_dag_id = f'computerease_procore_job_totals_sync_main_{instance}'
job_totals_child_dag_id = f'computerease_procore_job_totals_sync_child_{instance}'
budget_line_item_sync_child_dag_id = f'computerease_procore_budget_line_item_sync_child_{instance}'
contract_line_items_sync_child_dag_id = f'computerease_procore_contract_line_items_sync_child_{instance}'
contract_line_items_deletion_child_dag_id = f'computerease_procore_contract_line_items_deletion_sync_child_{instance}'
direct_cost_sync_child_dag_id = f'computerease_procore_direct_cost_sync_child_{instance}'

# Email configuration
tenant_email = ['mick.hodgins@fiicgc.com']
internal_email = ['procoreintegrationsupport@deltek.com']
s3_fingerprints_prefix = f'CE_Procore/{environment}/{instance}_'
s3_fingerprints_key = f'{s3_fingerprints_prefix}{s3_file_name}'  # pylint: disable=undefined-variable

disabled = True
