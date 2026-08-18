#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.user_rate_assignment_new_hire.config import *
instance = 'trial'
region = 'us-east-2'
environment = 'pre-production'
schedule_interval = '0 0 * * *'
execution_timeout_days = 14
report_name = 'Active project list - For Integration'

company_key = 'CBREFCGProductionafmig'
replicon_conn_id = 'cbrefcgafmig_replicon_apiuser'
master_dag_max_active_runs = 1
child_dag_active_runs= 1
child_dag_id = f'cbrefcgproduction_newhire_child_{instance}'
disabled = True
