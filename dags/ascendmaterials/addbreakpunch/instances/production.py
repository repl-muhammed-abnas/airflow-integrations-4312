#pylint: disable=wildcard-import unused-wildcard-import
from ascendmaterials.addbreakpunch.config import *
instance="production"
environment="production"
replicon_conn_id="ascendmaterials_replicon_admin"
company_key="ascendmaterials"
tenant_email = "schabo@ascendmaterials.com, rdbeal@ascendmaterials.com, aotten@ascendmaterials.com, klcars@ascendmaterials.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
