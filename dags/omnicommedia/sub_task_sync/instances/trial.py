# pylint: disable=wildcard-import unused-wildcard-import
from omnicommedia.sub_task_sync.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'OmnicomMediaafmig'
replicon_conn_id = 'replicon-OmnicomMediaafmig-automation'

hmac_secret_var = f'omnicommedia_webhook_subtask_sync_secret_{instance}'

can_redirect_to_workato_var_name = f'omnicommedia_subtask_sync_{instance}_redirect_to_workato'
workato_api_endpoint = f'omnicommedia_subtask_sync_{instance}_workato_endpoint'
workato_api_token_var_name = f'omnicommedia_subtask_sync_{instance}_workato_api_token'
can_run_batch_task_var_name = f'omnicommedia_subtask_sync_{instance}_can_run_batch_task'
omnicommedia_task_mapper = f'omnicommedia_task_mapper_{instance}'
disabled = True
