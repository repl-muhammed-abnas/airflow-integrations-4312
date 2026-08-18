# pylint: disable=wildcard-import unused-wildcard-import
from matlensilver.client_project_task_sync.config import *

instance = 'trial'
input_filepath = '/UAT/Project'
archive_filepath = '/UAT/Archive'
log_filepath = '/UAT/Log'
sftp_conn_id = 'sftp_matlensilver_586058'
tenant_email = 'IT@matlensilver.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
