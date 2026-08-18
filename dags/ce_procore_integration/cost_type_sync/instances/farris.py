# pylint: disable=wildcard-import
from ce_procore_integration.cost_type_sync.config import *

instance = 'farris'
region = 'us-east-1'
environment = 'pre-production'

procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

main_dag_id = f'computerease_procore_cost_type_sync_main_{instance}'
child_dag_id = f'computerease_procore_cost_type_sync_child_{instance}'

tenant_email = ['mick.hodgins@fiicgc.com']
internal_email = ['procoreintegrationsupport@deltek.com']
