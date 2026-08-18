# pylint: disable=wildcard-import unused-wildcard-import
from centricbrands.disable_user_enddate.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'CentricBrandsafmig'

replicon_conn_id = 'centricbrandsafmig_replicon_admin'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
disabled = True
