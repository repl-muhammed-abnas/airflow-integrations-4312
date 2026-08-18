# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.update_paid_invoice.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'
company_key = 'BCCSSTechnologyServices'
replicon_conn_id = 'BCCSSTechnologyServices_replicon_repliconsupport'
sftp_conn_id = "BCCSSTechnologyServices_sftp_Integration_GmailtoSFTP"
http_conn_id = "bccss_prod_create_invoice"
dag_max_active_tasks = 200
execution_timeout_days = 14
tenant_email = "TPOInvoices@hssbc.ca"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
can_run_batch_task_var_name = f"bccss_update_paid_invoices_{instance}_can_run_batch_task"
input_filepath = "/BCCSS/phsainvoicepaid/Input"
file_sensor_timeout = 10
token_var = f"bccss_{instance}_replicon_service_token"
from_address_file_path = "/BCCSS/phsainvoicepaid/fromaddress/"
archive_file_path = "/BCCSS/phsainvoicepaid/Archive"
