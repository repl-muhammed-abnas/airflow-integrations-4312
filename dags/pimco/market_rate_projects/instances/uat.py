# pylint: disable=wildcard-import unused-wildcard-import
from pimco.market_rate_projects.config import *

environment = 'pre-production'
instance = 'trial'
company_key = 'PIMCOTrial02'
replicon_conn_id = 'pimcotrial02-admin'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
