# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.timesheet_auto_submission_v2.config import *

instance = 'PwCDev'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCDev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'
timesheet_report_name = "**Timesheet_autosubmission_records**"

tenant_email = 'PWCGlobalLogs@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled=True
