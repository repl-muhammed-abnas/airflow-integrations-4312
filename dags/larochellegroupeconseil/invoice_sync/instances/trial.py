from larochellegroupeconseil.invoice_sync.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'panda2'
replicon_conn_id = 'larochelleGroupeConseil_replicon_adm.larochelle'

master_dag_id = 'larochelleGroupeconseil_invoicesync_master'
child_dag_id = 'larochelleGroupeconseil_invoicesync_update_invoice_child'


can_run_batch_task = f'LarochelleGroupeConseil_Invoicesync_{instance}_can_run_batch_task'
last_sync_time_variable = 'standard_LarochelleGroupeConseilafmig_invoice_last_sync_time'