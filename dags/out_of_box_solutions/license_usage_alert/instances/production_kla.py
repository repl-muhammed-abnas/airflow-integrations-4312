# pylint: disable=wildcard-import unused-wildcard-import
from out_of_box_solutions.license_usage_alert.config import *

instance = "production"

region = 'us-east-1'
environment = 'production'
company_key = 'kla'

schedule_interval = '0 14 * * *'

replicon_conn_id = "kla-replicon-usagealert"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
