from deltek_vantagepoint_v2.project_sync.config import *

region = 'us-east-1'
environment = 'qa'
instance = 'qa'
replicon_conn_id = 'airflowqasandbox-replicon-admin'
company_key = f"airflowqasandbox{region.replace('-', '')}"

webhook_basicauth_username = f'deltek_vantagepoint_webhook_username_{company_key}'
webhook_basicauth_password = f'deltek_vantagepoint_webhook_password_{company_key}'
can_run_batch_task_var_name = f'vp_replicon_project_sync_can_run_batch_task_{instance}'
is_project_full_sync_var = f'vp_replicon_is_project_full_sync_{instance}'

project_sync_main_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_project_sync_main_{instance}'
project_sync_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_project_sync_child_{instance}'

# History logging configs
provider = 'vantagepoint'
workflow = 'project_sync'
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_vantagepoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

timesheet_field_oef_name_for_lc = 'Labor Codes'
enable_budget_labor_codes_level = True
budget_labor_codes_level = "TimesheetFields" # Task / TimesheetFields
