# pylint: disable=wildcard-import
from ce_procore_integration.purchase_order_sync.config import *
from ce_procore_integration.purchase_order_sync.utils.constants import InputSource
from ce_procore_integration.util_dags.instances.sushmamathi1 import wbs_code_creator_dag_id

instance = 'sushmamathi1'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'

main_dag_id = f'computerease_procore_purchase_order_sync_main_{instance}'
child_dag_id = f'computerease_procore_purchase_order_sync_child_{instance}'
sov_sync_dag_id = f'computerease_procore_purchase_order_sync_sov_{instance}'

input_source = InputSource.SFTP

file_path = '/ce_procore/purchase_order'
archive_file_path = f'{file_path}/archive'
po_report_filename = 'Purchase_Order_Report'

tenant_email = ['sushmamathi@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
