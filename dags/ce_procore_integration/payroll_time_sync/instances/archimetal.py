# pylint: disable=wildcard-import
from ce_procore_integration.payroll_time_sync.config import *

instance = 'archimetal'
region = 'us-east-1'
environment = 'production'

# Connection IDs
procore_conn_id = f'procore_{instance}'
computerease_conn_id = f'computerease_{instance}'

# DAG IDs
main_dag_id = f'computerease_procore_payroll_time_sync_main_{instance}'

# Email configuration
tenant_email = ['louisa@amw-inc.com']
internal_email = ['procoreintegrationsupport@deltek.com']

payroll_time_last_sync_time_var = f'ce_procore_payroll_time_last_sync_time_{instance}'
