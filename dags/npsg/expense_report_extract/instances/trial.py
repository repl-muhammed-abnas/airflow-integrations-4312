# pylint: disable=wildcard-import unused-wildcard-import
from npsg.expense_report_extract.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'NPSGafmig'

replicon_conn_id = 'npsgafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/Expense Export/'
export_filepath = '/Expense Export/Logs/Logfile'
disabled = True
