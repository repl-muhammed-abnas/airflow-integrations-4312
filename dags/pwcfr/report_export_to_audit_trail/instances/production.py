# pylint: disable=wildcard-import unused-wildcard-import
from pwcfr.report_export_to_audit_trail.config import *

region = 'eu-central-1'
instance = "production"
environment = 'production'
company_key = 'pwcfr'

replicon_conn_id = 'pwcfr_replicon_automation.user'
sftp_conn_id = "sftp_pwcfr_594688"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
input_filepath = '/PROD/MONITORING/'
