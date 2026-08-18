#pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.new_updated_issues_from_jira.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'zaloragroup'
replicon_conn_id = 'zaloragroup_replicon_zrtest'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

http_conn_id = 'zalora_http_jira'

can_run_batch_task_var_name = f'zaloragroup_jira_task_import_can_run_batch_task_{instance}'

child_dag_id = f'zaloragroup_process_jira_child_{instance}'
