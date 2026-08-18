#pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.task_import_from_global_fashion.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'zaloragroup'
replicon_conn_id = 'zaloragroup_replicon_zrtest'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

http_conn_id = 'zalora_http_globalfashionjira'

can_run_batch_task_var_name = f'zaloragroup_global_fashion_jira_task_import_can_run_batch_task_{instance}'

child_process_jira_dag_id = f'zaloragroup_process_global_fashion_jira_child_{instance}'
