# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.report_export_to_audit_trail.config import *

region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'
company_key = 'pwcfrafmig'

replicon_conn_id = 'pwcfrafmig_replicon_admin'
sftp_conn_id = "sftp_useast2"

schedule_interval = "0 0 * * *"
time_zone = "Europe/Paris"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/PROD/MONITORING'
disabled = True
