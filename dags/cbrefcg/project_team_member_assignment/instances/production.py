#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.project_team_member_assignment.config import *

instance = 'production'
region = 'us-east-2'
environment = 'production'

company_key = 'CBREFCGProduction'
replicon_conn_id = 'cbrefcg_replicon_apiuser'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

login_name = 'apiuser'

can_run_batch_task_var_name = f'cbrefcg_project_team_assignment_can_run_batch_task_{instance}'

child_dag_id = f'cbrefcg_project_team_bilingrate_update_child_{instance}'
