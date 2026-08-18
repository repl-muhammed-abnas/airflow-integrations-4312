# pylint: disable=wildcard-import unused-wildcard-import
from horizonmedia.timesheet_approval_for_disabled_users.config import *

region = 'us-east-1'
instance = 'production'
environment = 'production'

company_key = 'Horizonmedia'
replicon_conn_id = 'horizonmedia_repliconadmin_replicon'
sftp_conn_id = 'horizonmedia_client_sftp'

tenant_email = "gfraga@horizonmedia.com,Sgrandi@horizonmedia.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/Force approve timesheets for disabled users/Log files'
