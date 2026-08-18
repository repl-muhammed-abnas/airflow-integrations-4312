# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.daily_schedule_to_enable_disable_user.config import *

company_key = 'pwcfrafmig'
replicon_conn_id = 'pwcframig_replicon_admin.user'
instance = 'trial'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_error_alert_mail = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
