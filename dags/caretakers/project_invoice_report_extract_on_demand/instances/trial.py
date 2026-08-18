# pylint: disable=wildcard-import unused-wildcard-import
from caretakers.project_invoice_report_extract_on_demand.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'caretakersafmig'
sftp_conn_id = 'sftp_useast2'

replicon_conn_id = 'caretakersafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

bearer_token_var = f'caretakersafmig_project_invice_report_extract_{instance}_secret'

csv_filepath = "Caretakers/custominvoicereportextract/"

disabled=True
