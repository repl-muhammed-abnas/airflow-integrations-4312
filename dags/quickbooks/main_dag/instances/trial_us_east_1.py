# pylint: disable=wildcard-import unused-wildcard-import
from quickbooks.main_dag.config import *

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowsandbox-replicon-admin'

timezone_iana = 'America/Los_Angeles'

can_run_batch_task_var_name = f'standard_quickbooks_online_main_dag_{instance}_can_run_batch_task'

client_import_dag = f"standard_quickbooks_online_{region.replace('-', '_')}_client_import_{instance}"
invoice_export_dag = f"standard_quickbooks_online_{region.replace('-', '_')}_invoice_export_{instance}"
invoice_status_update_dag = f"standard_quickbooks_online_{region.replace('-', '_')}_invoice_status_update_{instance}"
