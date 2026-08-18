#pylint: disable=wildcard-import unused-wildcard-import
from ascendmaterials.addbreakpunch.config import *
instance="trial"
environment="pre-production"
replicon_conn_id="ascendmaterials_replicon_admin.user"
company_key="ascendmaterialsafmig"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
