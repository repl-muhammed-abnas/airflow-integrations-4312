# pylint: disable=wildcard-import
from procore_ce_integration.payroll_time_sync.config import *

instance = 'rochesterPlumbing'
environment = 'production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'procore_computerease_payroll_time_sync_main_{instance}'
child_dag_id = f'procore_computerease_payroll_time_sync_child_{instance}'

payroll_time_last_sync_time_var = f'procore_ce_payroll_time_last_sync_time_{instance}'

internal_email = ['procoreintegrationsupport@deltek.com']
tenant_email = ['MitchN@rochph.com', 'AngieK@rochph.com']
