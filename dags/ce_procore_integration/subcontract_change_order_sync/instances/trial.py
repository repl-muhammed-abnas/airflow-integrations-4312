# pylint: disable=wildcard-import
from ce_procore_integration.subcontract_change_order_sync.config import *
from ce_procore_integration.util_dags.instances.trial import wbs_code_creator_dag_id

instance = 'dev'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_subcontract_change_order_sync_main_{instance}'
child_dag_id = f'computerease_procore_subcontract_change_order_sync_child_{instance}'
change_order_line_item_sync_dag_id = f'computerease_procore_subcontract_change_order_line_item_sync_{instance}'
change_order_line_item_deletion_dag_id = f'computerease_procore_subcontract_change_order_line_item_deletion_{instance}'


# Email configuration
tenant_email = ['MPTeamReplicon@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

subcontract_change_order_last_sync_time_var = f'ce_procore_subcontract_change_order_last_sync_time_{instance}'
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'
initial_sync_time = '1970-01-01T00:00:00Z'

change_order_change_reason = 'Allowance'
sync_only_approved_change_orders = 'No'
