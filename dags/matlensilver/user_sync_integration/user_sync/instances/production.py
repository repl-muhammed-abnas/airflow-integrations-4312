# pylint: disable=wildcard-import unused-wildcard-import
from matlensilver.user_sync_integration.user_sync.config import *

instance = 'production'
environment = 'production'
company_key = 'MatlenSilver'

input_filepath = '/Prod/Users'
archive_filepath = '/Prod/Archive'
log_filepath = '/Prod/Log'
reference_file = '/Prod/Reference/user_sync_reference_file.csv'

replicon_conn_id = 'matlensilver_replicon_admin'
sftp_conn_id = 'sftp_matlensilver_586058'

user_sync_mapper = f"matlen_silver_user_sync_mapper_{instance}"
sick_time_off_mapper = f"matlen_silver_sick_timeoff_mapper_{instance}"

tenant_email = 'IT@matlensilver.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
