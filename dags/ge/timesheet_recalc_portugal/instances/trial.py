# pylint: disable=wildcard-import unused-wildcard-import
from ge.timesheet_recalc_portugal.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'GEafmig'
replicon_conn_id = 'GEafmig_replicon_admin'
sftp_conn_id = 'GEafmig_sftp_QWWj5RX1hdQ0'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
