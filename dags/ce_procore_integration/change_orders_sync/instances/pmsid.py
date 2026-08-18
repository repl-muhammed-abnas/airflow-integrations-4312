# pylint: disable=wildcard-import
from ce_procore_integration.change_orders_sync.config import *
from ce_procore_integration.util_dags.instances.pmsid import wbs_code_creator_dag_id

instance = 'pmsid'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'

interval_minutes = 1

main_dag_id = f'computerease_procore_change_order_sync_main_{instance}'
job_child_dag_id = f'computerease_procore_change_order_sync_job_child_{instance}'
rfc_child_dag_id = f'computerease_procore_change_order_sync_rfc_child_{instance}'

file_path = '/ce_procore/change_orders'
archive_filepath = '/ce_procore/change_orders/archive'

tenant_email = ['SiddhantrajSingh@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
