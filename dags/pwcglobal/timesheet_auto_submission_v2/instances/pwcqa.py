# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.timesheet_auto_submission_v2.config import *

instance = 'pwcqa'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcqa'
replicon_conn_id = 'pwcqa-replicon-eu.automation'

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled=True
