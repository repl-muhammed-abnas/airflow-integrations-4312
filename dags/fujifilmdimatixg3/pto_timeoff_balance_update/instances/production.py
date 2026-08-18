# pylint: disable=wildcard-import unused-wildcard-import
from fujifilmdimatixg3.pto_timeoff_balance_update.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'
company_key = 'FUJIFILMDimatixG3'

replicon_conn_id = 'FUJIFILMDimatixG3_replicon_admin'
sftp_conn_id = 'sftp_gmailToSFTP_Integration_GmailtoSFTP'

input_filepath = "/Fujifilm/fujifilmptoupdate/Input"
fromaddress_filepath = "/Fujifilm/fujifilmptoupdate/fromaddress"
archive_filepath = "/Fujifilm/fujifilmptoupdate/Archive"


tenant_email = "fdmxpayroll@fujifilm.com"
bcc_email = '{{ var.value.dagrun_internal_log_email }}'
cc_email = '{{ var.value.dagrun_failure_alert_email }}'

max_active_runs_child = 1
