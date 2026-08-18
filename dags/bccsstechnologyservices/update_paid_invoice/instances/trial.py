# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.update_paid_invoice.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'BCCSSTechnologyServicesafmig'
replicon_conn_id = 'BCCSSTechnologyServicesafmig'
sftp_conn_id = "BCCSSTechnologyServicesafmigSFTP"
http_conn_id = f"replicon_service_bccss{instance}_paid_invoice"
dag_max_active_tasks = 200
execution_timeout_days = 14
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
can_run_batch_task_var_name = f"bccss_update_paid_invoices_{instance}_can_run_batch_task"
input_filepath = "/test_invoice_update"
file_sensor_timeout = 10
token_var = f"bccss_{instance}_replicon_service_token"
from_address_file_path = "/test_invoice_update_fromaddress"
archive_file_path = "/test_invoice_update_archive"
disabled = True
