# pylint: disable=wildcard-import unused-wildcard-import
from frontdoorinc.timesheet_autopopulation.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'
company_key = 'frontdoorinc'
replicon_conn_id = "frontdoorinc_replicon_admin"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
