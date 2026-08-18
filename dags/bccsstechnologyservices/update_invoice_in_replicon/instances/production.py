# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.update_invoice_in_replicon.config import *

region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'BCCSSTechnologyServices'

replicon_conn_id = 'BCCSSTechnologyServices_replicon_repliconsupport'
sftp_conn_id = "BCCSSTechnologyServices_sftp_Integration_GmailtoSFTP"

input_filepath = '/BCCSS/phsainvoiceupdate/Input/'
archive_filepath = '/BCCSS/phsainvoiceupdate/fromaddress/'
new_filepath = '/BCCSS/phsainvoiceupdate/Archive/'

can_run_batch_task_child = f'bccsstechnologyservices_update_invoice_in_replicon_child_{instance}_can_run_batch_task'

tenant_email = "TPOInvoices@hssbc.ca"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
