from larochellegroupeconseil.invoice_sync.config import *

instance = 'prod'
environment = 'production'

company_key = 'larochellegroupeconseil'
replicon_conn_id = 'larochellegroupeconseil_replicon_adm.larochelle'

master_dag_id = f'larochellegroupeconseil_invoicesync_master_{instance}'
child_dag_id = f'larochellegroupeconseil_invoicesync_update_invoice_child_{instance}'


can_run_batch_task = f'larochellegroupeconseil_invoicesync_can_run_batch_task_{instance}'
last_sync_time_variable = f'standard_larochellegroupeconseilafmig_invoice_last_sync_time_{instance}'
