# pylint: disable=wildcard-import unused-wildcard-import
from pimco.market_rate_projects.config import *

environment = 'production'
instance = 'production'
company_key = 'PIMCO'
replicon_conn_id = 'pimco-replicon-production'


tenant_email = 'james.stone@pimco.com,david.edwards@pimco.com,alexandria.rausch@pimco.com,\
scott.schwarmann@pimco.com,shekhar.gupta@pimco.com,mayank.sharma@pimco.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
