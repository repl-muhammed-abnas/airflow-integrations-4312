# pylint: disable=wildcard-import unused-wildcard-import
from lendingclub.user_import.config import *
instance = 'trial'
company_key = 'LendingClubafmig'

replicon_conn_id = 'lendingclubafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = "pgp_lendingclub_userimport"

input_filepath = 'LendingClub/User Import/Input'

input_filepath_master = '/LendingClub/User Import/Processing'
log_filepath = '/LendingClub/User Import/Logs'
archive_filepath = '/LendingClub/User Import/Archive'

to_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'lendingclubafmig_user_import_{instance}_can_run_batch_task'

disable=True

disabled=True
