# pylint: disable=wildcard-import unused-wildcard-import
from tungstenconstructionllc.payroll_export_replicon_to_sftp.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'tungstenconstructionllcafmig'
sftp_conn_id = 'sftp_useast2'

replicon_conn_id = 'tungstenconstructionllcafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

bearer_token_var = f'tungstenconstructionllc_payroll_export_{instance}_secret'

csv_filepath = "TungstenConstructionLLC/payrollandperdiemextract"
