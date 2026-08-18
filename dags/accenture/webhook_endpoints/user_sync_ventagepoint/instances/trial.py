# pylint: disable=wildcard-import unused-wildcard-import
from accenture.webhook_endpoints.user_sync_ventagepoint.config import *

instance = "trial"
environment = "pre-production"

company_key = "Accenture"

vantagepoint_conn_id = "accenture_vantagepoint_tus"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

webhook_master_dagid = f"accenture_user_sync_mrdr_webhook_master_{instance}"

basic_auth_username_accenture_mrdr = f"accenture_user_sync_mrdr_webhook_username_{instance}"
basic_auth_password_accenture_mrdr = f"accenture_user_sync_mrdr_webhook_password_{instance}"

can_run_batch_task_var_name = f'accenture_user_sync_can_run_batch_task_{instance}'

process_employee_child_dag_id = f'accenture_user_sync_mrdr_child_{instance}'

disabled=True
