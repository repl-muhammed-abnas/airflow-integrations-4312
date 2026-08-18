# pylint: disable=wildcard-import unused-wildcard-import
from velaw.overtime_export.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'
company_key = 'Velaw'

replicon_conn_id = 'velaw_replicon_Rintegrations'
sftp_conn_id = "sftp_velaw_524663"

tenant_email = "Replicon-Firm@Velaw.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/CMSExport/Production/'
