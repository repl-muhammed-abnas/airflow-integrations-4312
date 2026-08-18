# pylint: disable=wildcard-import unused-wildcard-import
from ce_procore_integration.subcontract_sync_v2.config import *
from ce_procore_integration.util_dags.instances.pmdemo1 import wbs_code_creator_dag_id

instance = "pmdemo1"
environment = 'pre-production'

# Connection IDs for pre-production environment
computerease_conn_id = f'computerease_{instance}'
procore_conn_id = f'procore_{instance}'

subcontract_main_dag_id = f'computerease_procore_subcontract_sync_main_v2_{instance}'
subcontract_per_project_child_dag_id = f'computerease_procore_subcontract_per_project_v2_{instance}'
subcontract_vendor_assignment_child_dag_id = f'computerease_procore_subcontract_vendor_assignment_v2_{instance}'
subcontract_child_dag_id = f'computerease_procore_subcontract_sync_child_v2_{instance}'
subcontract_line_items_child_dag_id = f'computerease_procore_subcontract_line_items_sync_child_v2_{instance}'
subcontract_line_items_deletion_child_dag_id = f'computerease_procore_subcontract_line_items_deletion_child_v2_{instance}'

subcontract_last_sync_time_var = f'ce_procore_subcontract_sync_last_sync_time_{instance}'

# Email configuration
tenant_email = ['timmattlin@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
