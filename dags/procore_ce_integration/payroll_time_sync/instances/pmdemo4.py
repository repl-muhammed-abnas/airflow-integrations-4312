# pylint: disable=wildcard-import
from procore_ce_integration.payroll_time_sync.config import *

instance = 'pmdemo4'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'procore_computerease_payroll_time_sync_main_{instance}'
child_dag_id = f'procore_computerease_payroll_time_sync_child_{instance}'

# Email configuration
tenant_email = ['christinehill@deltek.com']
internal_email = ['MPTeamReplicon@deltek.com']

payroll_time_last_sync_time_var = f'procore_ce_payroll_time_last_sync_time_{instance}'
