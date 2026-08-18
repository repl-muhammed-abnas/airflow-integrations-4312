# pylint: disable=wildcard-import
from ce_procore_integration.change_orders_sync.config import *
from ce_procore_integration.util_dags.instances.trial import wbs_code_creator_dag_id

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

input_source = 'email'

# Email configuration (used when input_source = 'email')
imap_conn_id = 'computerease_procore_imap'
email_subject_pattern = 'Change Order Report'
email_limit = 1
email_max_to_check = 100

main_dag_id = f'computerease_procore_change_order_sync_main_{instance}'
job_child_dag_id = f'computerease_procore_change_order_sync_job_child_{instance}'
rfc_child_dag_id = f'computerease_procore_change_order_sync_rfc_child_{instance}'

tenant_email = ['MPTeamReplicon@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
