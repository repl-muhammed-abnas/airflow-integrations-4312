# pylint: disable=wildcard-import unused-wildcard-import
from npsg.expense_report_extract.config import *

instance = "production"
environment = 'production'
company_key = 'NPSG'

replicon_conn_id = 'npsg_replicon_admin'
sftp_conn_id = "sftp_npsg_610439"

tenant_email = "replicon@npsgglobal.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/Expense Export/'
export_filepath = '/Expense Export/Logs/'
