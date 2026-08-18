# pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.user_import_v1.config import *
instance = 'trial'
company_key = 'zaloragroupafmig'

replicon_conn_id = 'zaloragroupafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = "pgp_zaloragroup_userimport"

to_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'zaloragroupafmig_user_import_{instance}_can_run_batch_task'

input_filepath = 'Zaloragroup/User Import'

input_filepath_master = '/Zaloragroup/User Import/Processing'
log_filepath = '/Zaloragroup/User Import/Logs'
archive_filepath = '/Zaloragroup/User Import/Archive'
