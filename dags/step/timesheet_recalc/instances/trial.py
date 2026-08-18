# pylint: disable=wildcard-import unused-wildcard-import
from step.timesheet_recalc.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'Stepafmig'
replicon_conn_id = 'Stepafmig_replicon_admin'

run_date_var = f"{company_key}_{instance}_run_date"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_bcc = '{{ var.value.dagrun_internal_testing_email }}'
tenant_cc = '{{ var.value.dagrun_internal_testing_email }}'

disabled=True
