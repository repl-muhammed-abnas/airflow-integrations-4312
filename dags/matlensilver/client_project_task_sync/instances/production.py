# pylint: disable=wildcard-import unused-wildcard-import
from matlensilver.client_project_task_sync.config import *

instance = 'production'
environment = 'production'
company_key = 'MatlenSilver'
replicon_conn_id = 'matlensilver_replicon_admin'
input_filepath = '/Prod/Project'
archive_filepath = '/Prod/Archive'
log_filepath = '/Prod/Log'
sftp_conn_id = 'sftp_matlensilver_586058'
tenant_email = 'IT@matlensilver.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
disabled = True
