# pylint: disable=wildcard-import unused-wildcard-import
from avenu.user_import.config import *

instance = 'production'
environment = 'production'
company_key = "avenuinsightsanalytics"

input_filepath = '/Production/User Import/Input'
archive_filepath = '/Production/User Import/Archive'
log_filepath = '/Production/User Import/Log'
reference_file = '/Production/User Import/Reference/user_sync_reference_file.csv'

replicon_conn_id = 'avenuinsightsanalytics_replicon_admin'
sftp_conn_id = 'sftp_avenuinsightsanalytics_659432'


user_sync_mapper = f"avenu_user_sync_mapper_{instance}"
payrule_sync_mapper = f"avenu_payrule_sync_mapper_{instance}"

tenant_email = 'BU-Operations@avenuinsights.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"avenu_user_import_{instance}_can_run_batch_task"
