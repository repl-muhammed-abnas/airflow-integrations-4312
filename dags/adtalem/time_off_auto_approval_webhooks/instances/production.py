# pylint: disable=wildcard-import unused-wildcard-import
from adtalem.time_off_auto_approval_webhooks.config import *

region = 'us-east-1'
instance = 'production'
environment = 'production'

company_key = 'adtalem'
replicon_conn_id = 'adtalem-replicon-integration.user'

execution_timeout_days = 14

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

webhook_shared_secrate = f"adtalem_timeoff_auto_approval_webhooks_secrate_key_{instance}"
