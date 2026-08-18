#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.user_rate_assignment_new_hire.config import *
instance = 'production'

region = 'us-east-2'
environment = 'production'

company_key = 'CBREFCGProduction'
replicon_conn_id = 'cbrefcg_replicon_apiuser'

report_name = 'Active project list - For Integration'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

child_dag_id = f'cbrefcg_newhire_child_{instance}'
