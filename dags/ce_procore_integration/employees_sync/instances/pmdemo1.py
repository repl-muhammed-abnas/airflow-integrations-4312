# pylint: disable=wildcard-import unused-wildcard-import
from ce_procore_integration.employees_sync.config import *

instance = 'pmdemo1'
region = 'us-east-1'
environment = 'pre-production'

# Connection IDs
computerease_conn_id = f'computerease_{instance}'
procore_conn_id = f'procore_{instance}'

employee_main_dag_id = f'computerease_procore_employee_sync_main_{instance}'
employee_child_dag_id = f'computerease_procore_employee_sync_child_{instance}'

employee_last_sync_time_var = f'ce_procore_employee_sync_last_sync_time_{instance}'

# Email configuration
tenant_email = ['timothymattlin@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']
