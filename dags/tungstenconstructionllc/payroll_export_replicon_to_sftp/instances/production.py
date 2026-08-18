# pylint: disable=wildcard-import unused-wildcard-import
from tungstenconstructionllc.payroll_export_replicon_to_sftp.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'tungstenconstructionllc'
sftp_conn_id = 'sftp_tungstenconstructionllc_admin'

replicon_conn_id = 'tungstenconstructionllc_replicon_admin'

tenant_email = "jean@tungstenconst.com,marina@tungstenconst.com,tania@tungstenconst.com"
bcc_email = '{{ var.value.dagrun_internal_log_email }}'

bearer_token_var = f'tungstenconstructionllc_payroll_export_{instance}_secret'

csv_filepath = "/payrollandperdiemextract"
