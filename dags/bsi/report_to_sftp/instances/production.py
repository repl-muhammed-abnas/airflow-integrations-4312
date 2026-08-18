# pylint: disable=wildcard-import unused-wildcard-import
from bsi.report_to_sftp.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'BSI'
replicon_conn_id = 'bsi_replicon_Lindsay'
sftp_conn_id = 'bsi_sftp_635768'

tenant_email = "will.johnson-marshall@bsigroup.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
