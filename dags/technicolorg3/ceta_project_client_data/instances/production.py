# pylint: disable=wildcard-import unused-wildcard-import
from technicolorg3.ceta_project_client_data.config import *

instance = "production"
environment = 'production'
company_key = 'TechnicolorG3'
replicon_conn_id = 'replicon-technicolorG3-admin'

tenant_email = "psadvreplicon-support@technicolor.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'technicolor_webhook_ceta_project_client_secret_{instance}'

can_redirect_to_workato_var_name = f'technicolor_project_client_{instance}_redirect_to_workato'
workato_api_endpoint = f'technicolor_project_client_{instance}_workato_endpoint'
workato_api_token_var_name = f'technicolor_project_client_{instance}_workato_api_token'
can_run_batch_task_var_name = f'technicolor_project_client_{instance}_can_run_batch_task'

project_tasks_mapper = f'technicolor_project_tasks_mapper_{instance}'
disabled = True
