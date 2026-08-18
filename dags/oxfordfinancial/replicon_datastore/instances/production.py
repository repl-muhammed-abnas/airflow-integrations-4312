# pylint: disable=wildcard-import unused-wildcard-import
from oxfordfinancial.replicon_datastore.config import *

instance = 'production'
environment = 'production'
company_key = 'oxfordfinancial'

replicon_conn_id = 'oxfordfinancial_replicon_admin1'
sftp_conn_id = 'sftp_oxfordfinancial_629141'

tenant_email = 'mellis@ofgltd.com,integrationuser@ofgltd.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

time_entry_id_var_name = f'oxfordfinancial_replicon_datastore_{instance}_time_entry_id'
can_run_batch_task_var_name = f'oxfordfinancial_replicon_datastore_{instance}_can_run_batch_task'
