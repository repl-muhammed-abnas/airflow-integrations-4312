# pylint: disable=wildcard-import unused-wildcard-import
from centricbrands.disable_user_enddate.config import *

region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'CentricBrands'

replicon_conn_id = 'centricbrands_replicon_admin'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
