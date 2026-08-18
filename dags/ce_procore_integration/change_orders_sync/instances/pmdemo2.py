# pylint: disable=wildcard-import
from ce_procore_integration.change_orders_sync.config import *
from ce_procore_integration.util_dags.instances.pmdemo2 import wbs_code_creator_dag_id

instance = 'pmdemo2'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'
sftp_conn_id = f'ce_procore_sftp_{instance}'

main_dag_id = f'computerease_procore_change_order_sync_main_{instance}'
job_child_dag_id = f'computerease_procore_change_order_sync_job_child_{instance}'
rfc_child_dag_id = f'computerease_procore_change_order_sync_rfc_child_{instance}'

input_source = 'sftp'

file_path = '/ce_procore/change_orders'
archive_filepath = f'{file_path}/archive'
co_report_filename = 'QTool Change Order Report'
job_cost_detail_report_filename = 'QTool Job Cost Detail'

tenant_email = ['christinehill@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
