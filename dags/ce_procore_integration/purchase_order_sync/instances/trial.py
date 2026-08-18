# pylint: disable=wildcard-import
from ce_procore_integration.purchase_order_sync.config import *
from ce_procore_integration.util_dags.instances.trial import wbs_code_creator_dag_id

instance = 'dev'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
imap_conn_id = 'ce_procore_imap'

input_source = InputSource.EMAIL

# DAG IDs
main_dag_id = f'computerease_procore_purchase_order_sync_main_{instance}'
child_dag_id = f'computerease_procore_purchase_order_sync_child_{instance}'
sov_sync_dag_id = f'computerease_procore_purchase_order_sync_sov_{instance}'

# Email configuration
tenant_email = ['MPTeamReplicon@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
