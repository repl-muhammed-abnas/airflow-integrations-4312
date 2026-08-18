# pylint: disable=wildcard-import unused-wildcard-import
from frontdoorinc.timesheet_autopopulation.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'frontdoorincafmig'
replicon_conn_id = "frontdoorincafmig_replicon_admin"

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
