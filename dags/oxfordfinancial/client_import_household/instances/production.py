# pylint: disable=wildcard-import unused-wildcard-import
from oxfordfinancial.client_import_household.config import *

instance = 'production'
environment = 'production'
company_key = 'oxfordfinancial'

replicon_conn_id = 'oxfordfinancial_replicon_admin1'
sftp_conn_id = 'sftp_oxfordfinancial_629141'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'oxfordfinancial_client_import_household_{instance}_can_run_batch_task'
