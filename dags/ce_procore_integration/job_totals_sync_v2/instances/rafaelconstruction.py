# pylint: disable=wildcard-import,unused-import
from ce_procore_integration.job_totals_sync_v2.config import *
from ce_procore_integration.util_dags.instances.rafaelconstruction import wbs_code_creator_dag_id

instance = 'rafaelconstruction'
environment = 'production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

job_totals_main_dag_id = f'computerease_procore_job_totals_sync_main_v2_{instance}'
job_totals_per_job_child_dag_id = f'computerease_procore_job_totals_per_job_child_v2_{instance}'
budget_sync_child_dag_id = f'computerease_procore_budget_sync_child_v2_{instance}'
contract_line_items_sync_child_dag_id = f'computerease_procore_contract_line_items_sync_child_v2_{instance}'
contract_line_items_deletion_child_dag_id = f'computerease_procore_contract_line_items_deletion_sync_child_v2_{instance}'
direct_cost_sync_child_dag_id = f'computerease_procore_direct_cost_sync_child_v2_{instance}'

# Email configuration
tenant_email = ['Nichole@RafaelCompanies.com']
internal_email = ['procoreintegrationsupport@deltek.com']
s3_fingerprints_prefix = f'CE_Procore/{environment}/{instance}_'
s3_fingerprints_key = f'{s3_fingerprints_prefix}{s3_file_name}'  # pylint: disable=undefined-variable
