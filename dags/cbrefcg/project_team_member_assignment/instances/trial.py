#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.project_team_member_assignment.config import *

instance = 'trial'
region = 'us-east-2'
environment = 'pre-production'

company_key = 'CBREFCGProductionafmig'
replicon_conn_id = 'cbrefcgafmig_replicon_apiuser'

schedule_interval = '0 */2 * * *'

login_name = 'apiuser'

can_run_batch_task_var_name = f'cbrefcg_project_team_assignment_can_run_batch_task_{instance}'

child_dag_id = f'cbrefcg_project_team_bilingrate_update_child_{instance}'
disabled = True
