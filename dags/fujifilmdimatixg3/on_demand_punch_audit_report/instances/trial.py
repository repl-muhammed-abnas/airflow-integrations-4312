# pylint: disable=wildcard-import unused-wildcard-import
from fujifilmdimatixg3.on_demand_punch_audit_report.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'FUJIFILMDimatixG3afmig'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

replicon_conn_id = 'fujifilmdimatixg3afmig_replicon_admin'
bearer_token_var = f'fujifilmdimatix_punch_audit_report_{instance}_secret'

# disabled=True
