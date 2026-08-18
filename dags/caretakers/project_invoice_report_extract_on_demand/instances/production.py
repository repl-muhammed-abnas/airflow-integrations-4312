# pylint: disable=wildcard-import unused-wildcard-import
from caretakers.project_invoice_report_extract_on_demand.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'caretakers'
sftp_conn_id = 'sftp_caretakers_admin'

replicon_conn_id = 'caretakers_replicon_admin'

tenant_email = "accounting@splatsindc.com,gabriela.rodriguez@splatsindc.com,danielle.burke@splatsindc.com"
bcc_email = '{{ var.value.dagrun_internal_log_email }}'

bearer_token_var = f'caretakers_project_invice_report_extract_{instance}_secret'

csv_filepath = "/custominvoicereportextract/"
