# pylint: disable=wildcard-import unused-wildcard-import
from adtalem.time_off_auto_approval_webhooks.config import *

region = 'us-east-1'
instance = 'pre-production'
environment = 'pre-production'

company_key = 'Adtalemtrial01'
replicon_conn_id = 'Adtalemtrial01-replicon-admin'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

webhook_shared_secrate = f"adtalem_timeoff_auto_approval_webhooks_secrate_key_{instance}"
disabled = True
