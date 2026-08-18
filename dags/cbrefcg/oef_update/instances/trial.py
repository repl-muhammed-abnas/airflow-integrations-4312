#pylint: disable=wildcard-import unused-wildcard-import
from cbrefcg.oef_update.config import *

instance = 'trial'
region = 'us-east-2'
environment = 'pre-production'

company_key = 'CBREFCGProductionafmig'
replicon_conn_id = 'cbrefcgafmig_replicon_apiuser'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'cbrefcg_oef_update_can_run_batch_task_{instance}'

child_dag_id = f'cbrefcg_processing_each_oef_child_{instance}'
disabled = True
