# pylint: disable=wildcard-import
from ce_procore_integration.job_structure_sync.config import *

instance = 'daniellesales'
region = 'us-east-1'
environment = 'pre-production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

job_main_dag_id = f'computerease_procore_job_sync_main_{instance}'
job_child_dag_id = f'computerease_procore_job_sync_child_{instance}'
job_child_dag_v2_id = f'computerease_procore_job_sync_child_v2_{instance}'

phases_child_dag_id = f'computerease_procore_phases_sync_child_{instance}'
category_child_dag_id = f'computerease_procore_category_sync_child_{instance}'
prime_contract_child_dag_id = f'computerease_procore_prime_contract_sync_child_{instance}'

job_last_sync_time_var = f'ce_procore_job_sync_last_sync_time_{instance}'

# Email configuration
tenant_email = ['DanielleMottl@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
