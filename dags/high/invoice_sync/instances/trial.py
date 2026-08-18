from high.invoice_sync.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'OperationalSustainabilityafmig'
replicon_conn_id = 'OperationalSustainabilityafmig_replicon_david.drerup'

xero_conn_id = 'trial_xero_janapatipushpa'

master_dag_id = f'high_invoice_export_master_{instance}'
child_dag_id = f'high_invoice_export_add_invoice_child_{instance}'


can_run_batch_task = f'High_Invoicesync_{instance}_can_run_batch_task'
last_sync_time_variable = 'standard_High_invoice_last_sync_time'


notification_email ="{{ var.value.dagrun_internal_testing_email }}"

