# pylint: disable=wildcard-import
from ce_procore_integration.change_orders_sync.config import *
from ce_procore_integration.util_dags.instances.johnmsales import wbs_code_creator_dag_id

instance = 'johnmsales'


# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_change_order_sync_main_{instance}'
job_child_dag_id = f'computerease_procore_change_order_sync_job_child_{instance}'
rfc_child_dag_id = f'computerease_procore_change_order_sync_rfc_child_{instance}'

# Email input source configuration
input_source = 'email'
imap_conn_id = f'computerease_procore_imap_{instance}'

# Email notification configuration
tenant_email = ['johnmeibers@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']