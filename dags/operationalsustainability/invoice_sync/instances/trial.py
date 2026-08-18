from operationalsustainability.invoice_sync.config import *

instance = 'trial'
environment = 'production'

company_key = 'OperationalSustainabilitytrial'
replicon_conn_id = 'OperationalSustainabilityafmig_replicon_david.drerup'

qbo_conn_id = 'OperationalSustainabilityafmig-quickbooks-apiuser'

master_dag_id = f'operationalsustainability_invoice_export_master_{instance}'
child_dag_id = f'operationalsustainability_invoice_export_add_invoice_child_{instance}'
invoice_items_loop_dag_id = f'operationalsustainability_expense_export_invoice_items_loop_child_{instance}'


can_run_batch_task = f'OperationalSustainability_Invoicesync_{instance}_can_run_batch_task'
last_sync_time_variable = 'standard_OperationalSustainability_Invoicesync_last_sync_time'

notification_email = "{{ var.value.dagrun_internal_testing_email }}"
