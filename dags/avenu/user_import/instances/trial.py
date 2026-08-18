# pylint: disable=wildcard-import unused-wildcard-import
from avenu.user_import.config import *

instance = 'trial'
environment = 'pre-production'
input_filepath = '/Trial/User Import/Input'
archive_filepath = '/Trial/User Import/Archive'
log_filepath = '/Trial/User Import/Log'
reference_file = '/Trial/User Import/Reference/user_sync_reference_file.csv'

replicon_conn_id = 'avenuinsightsanalyticstrial01_replicon_admin'
sftp_conn_id = 'sftp_useast2'

user_sync_mapper = f"avenu_user_sync_mapper_{instance}"
payrule_sync_mapper = f"avenu_payrule_sync_mapper_{instance}"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"avenu_user_import_{instance}_can_run_batch_task"

disable=True

disabled=True
