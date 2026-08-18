# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.create_invoices_for_each_project_code.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'bccsstechnologyservicesafmig'
replicon_conn_id = 'bccsstechnologyservicesafmig'
http_conn_id = "replicon_service_bccsstrial_paid_invoice"
sftp_conn_id = 'replicol_sftp'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

schedule_interval = 300
execution_timeout_days = 14

input_file_path = "/invoicesync/singleinvoice/Input"
input_file_archive_path = "/invoicesync/singleinvoice/Input/archive"
email_file_path = "/invoicesync/singleinvoice/toaddress"
email_file_archive_path = "/invoicesync/singleinvoice/toaddress/archive"


token_var = f"bccss_{instance}_replicon_service_token"
disabled = True
