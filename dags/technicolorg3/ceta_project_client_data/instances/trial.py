# pylint: disable=wildcard-import unused-wildcard-import
from technicolorg3.ceta_project_client_data.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'technicolorg3afmig'
replicon_conn_id = 'replicon-technicolorg3afmig-admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearer_token_var = f'technicolor_webhook_ceta_project_client_secret_{instance}'

can_redirect_to_workato_var_name = f'technicolor_project_client_{instance}_redirect_to_workato'
workato_api_endpoint = f'technicolor_project_client_{instance}_workato_endpoint'
workato_api_token_var_name = f'technicolor_project_client_{instance}_workato_api_token'
can_run_batch_task_var_name = f'technicolor_project_client_{instance}_can_run_batch_task'

project_tasks_mapper = f'technicolor_project_tasks_mapper_{instance}'

disable=True

disabled=True
