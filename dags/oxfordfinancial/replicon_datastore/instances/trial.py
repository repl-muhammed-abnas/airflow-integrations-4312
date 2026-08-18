# pylint: disable=wildcard-import unused-wildcard-import
from oxfordfinancial.replicon_datastore.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'oxfordfinancialafmig'

replicon_conn_id = 'oxfordfinancialafmig-replicon-admin1'
sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

time_entry_id_var_name = f'oxfordfinancial_replicon_datastore_{instance}_time_entry_id'
can_run_batch_task_var_name = f'oxfordfinancial_replicon_datastore_{instance}_can_run_batch_task'
disabled = True
