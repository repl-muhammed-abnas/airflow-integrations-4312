# pylint: disable=wildcard-import unused-wildcard-import
from tungstenconstructionllc.project_costing_report.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'TungstenConstructionLLCafmig'
replicon_conn_id = 'TungstenConstructionLLCafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

bearer_token_var = f'tungsten_construction_llc_project_export_{instance}_secret'
