# pylint: disable=wildcard-import unused-wildcard-import
from accenture.payroll_integration.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'Accenturesandbox'

vantagepoint_conn_id = 'accenturesandbox_vantagepoint_tus_payroll'
sftp_conn_id = 'accenturesandbox_sftp_payroll'
pgp_conn_id = 'pgp_accenturesandbox_adp_gv'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'accenture_payroll_can_run_batch_task_{instance}'

process_payroll_child_dag_id = f'accenture_payroll_mrdr_child_{instance}'
