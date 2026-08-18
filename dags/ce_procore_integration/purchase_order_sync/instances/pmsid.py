# pylint: disable=wildcard-import
from ce_procore_integration.purchase_order_sync.config import *
from ce_procore_integration.util_dags.instances.pmsid import wbs_code_creator_dag_id

instance = 'pmsid'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_purchase_order_sync_main_{instance}'
child_dag_id = f'computerease_procore_purchase_order_sync_child_{instance}'
sov_sync_dag_id = f'computerease_procore_purchase_order_sync_sov_{instance}'

# Project configuration
file_path = '/ce_procore/purchase_order'
archive_file_path = f'{file_path}/archive'

# Email configuration
tenant_email = ['SiddhantrajSingh@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
