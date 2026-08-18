# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.create_invoices_for_each_project_code.config import *

instance = 'production'
environment = 'production'

company_key = 'BCCSSTechnologyServices'
replicon_conn_id = 'BCCSSTechnologyServices_replicon_repliconsupport'
http_conn_id = "bccss_prod_create_invoice"
sftp_conn_id = 'BCCSSTechnologyServices_sftp_Integration_GmailtoSFTP'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

schedule_interval = 300
execution_timeout_days = 14

input_file_path = "/BCCSS/bccssinvoice_single/Input"
input_file_archive_path = "/BCCSS/bccssinvoice_single/Archive/"
email_file_path = "/BCCSS/bccssinvoice_single/fromaddress"
email_file_archive_path = "/BCCSS/bccssinvoice_single/Archive/"

token_var = f"bccss_{instance}_replicon_service_token"
