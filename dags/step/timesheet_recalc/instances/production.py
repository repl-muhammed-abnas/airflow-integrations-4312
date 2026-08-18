# pylint: disable=wildcard-import unused-wildcard-import
from step.timesheet_recalc.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'STEP'
replicon_conn_id = 'STEP_replicon_admin'

run_date_var = f"{company_key}_{instance}_run_date"

tenant_email = "Replicon@step-es.com,Timesheets@step-es.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_cc = "Payroll@step-es.com,Anita.Suri@step-es.com"
tenant_bcc = '{{ var.value.dagrun_internal_log_email }}'

