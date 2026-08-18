# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.update_invoice_in_replicon.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'bccsstechnologyservicesafmig'

replicon_conn_id = 'bccsstechnologyservicesafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

input_filepath = '/BCCSS/phsainvoiceupdate/Input'
archive_filepath = '/BCCSS/phsainvoiceupdate/fromaddress/'
new_filepath = '/BCCSS/phsainvoiceupdate/Archive/'

can_run_batch_task_child = f'bccsstechnologyservices_update_invoice_in_replicon_child_{instance}_can_run_batch_task'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
