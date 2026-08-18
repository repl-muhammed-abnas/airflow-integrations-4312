# pylint: disable=wildcard-import unused-wildcard-import
from tungstenconstructionllc.project_costing_report.config import *

instance = "prod"
environment = 'production'
company_key = 'TungstenConstructionLLC'
replicon_conn_id = 'tungstenconstructionll_replicon_admin'

tenant_email = "jean@tungstenconst.com,marina@tungstenconst.com"
bcc_tenant_email =  '{{ var.value.dagrun_internal_log_email }}'

bearer_token_var = f'tungstenconstructionllc_project_export_{instance}_secret'
