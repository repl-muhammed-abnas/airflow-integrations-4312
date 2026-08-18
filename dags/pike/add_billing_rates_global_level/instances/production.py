# pylint: disable=wildcard-import unused-wildcard-import
from pike.add_billing_rates_global_level.config import *

instance = 'production'
environment = 'production'

company_key = 'pike'

replicon_conn_id = 'pike-replicon-admin'
sftp_conn_id = 'sftp_gmailToSFTP_Integration_GmailtoSFTP'

input_filepath = "/Pike/Pike.billingrate/Input"
archive_filepath = "/Pike/Pike.billingrate/Archive"
email_id_path = "/Pike/Pike.billingrate/fromaddress"

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
