# pylint: disable=wildcard-import unused-wildcard-import
from accenture.webhook_endpoints.payroll_integration_ventagepoint.config import *

instance = "trial"
environment = "pre-production"

company_key = "Accenture"

vantagepoint_conn_id = "accenture_vantagepoint_tus"

webhook_master_dagid = f"accenture_payroll_mrdr_webhook_master_{instance}"

basic_auth_username_accenture_mrdr = f"accenture_payroll_mrdr_webhook_username_{instance}"
basic_auth_password_accenture_mrdr = f"accenture_payroll_mrdr_webhook_password_{instance}"

can_run_batch_task_var_name = f'accenture_payroll_can_run_batch_task_{instance}'

process_payroll_child_dag_id = f'accenture_payroll_mrdr_child_{instance}'
