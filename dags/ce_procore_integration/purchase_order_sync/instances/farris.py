# pylint: disable=wildcard-import
from ce_procore_integration.purchase_order_sync.config import *
from ce_procore_integration.purchase_order_sync.utils.constants import InputSource
from ce_procore_integration.util_dags.instances.farris import wbs_code_creator_dag_id

instance = 'farris'
region = 'us-east-1'
environment = 'pre-production'


# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_purchase_order_sync_main_{instance}'
child_dag_id = f'computerease_procore_purchase_order_sync_child_{instance}'
sov_sync_dag_id = f'computerease_procore_purchase_order_sync_sov_{instance}'

# Email input source configuration
imap_conn_id = f'computerease_procore_imap_{instance}'
input_source = InputSource.EMAIL

# Email notification configuration
tenant_email = ['mick.hodgins@fiicgc.com']
internal_email = ['procoreintegrationsupport@deltek.com']

email_subject_pattern = 'Procore Purchase Order List'