# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.daily_schedule_to_enable_disable_user.config import *

company_key = 'pwcfr'
environment="production"
replicon_conn_id = 'pwcfr_replicon_admin'
instance = 'production'
alert_email = '{{ var.value.dagrun_internal_log_email }}'
bcc_error_alert_mail ='{{ var.value.dagrun_failure_alert_email }}'
