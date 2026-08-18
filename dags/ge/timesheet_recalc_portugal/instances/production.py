# pylint: disable=wildcard-import unused-wildcard-import
from ge.timesheet_recalc_portugal.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'GE'
replicon_conn_id = 'ge_replicon_admin'
sftp_conn_id = 'ge_sftp_QWWj5RX1hdQ0'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
