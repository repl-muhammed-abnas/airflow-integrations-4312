# pylint: disable=wildcard-import unused-wildcard-import
from ttecholdingsinc.schedule_creation_v1.config import *

region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'TTECHOLDINGSINCTRIAL01'

replicon_conn_id = 'ttecholdingsinctrial01_replicon_admin'
sftp_conn_id = 'rsftp-useast_for_testing'

input_filepath = '/TTEC/Shift Schedule/input'
archive_filepath = '/TTEC/Shift Schedule/archive'
log_filepath = '/TTEC/Shift Schedule/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name =  f'ttec_shift_schedule_import_{instance}_can_run_batch_task'

create_schedule_dag_id = f'ttec_schedule_creation_child_{instance}_v1'
shift_child_dag_id = f'ttec_process_each_shift_schedule_child_{instance}_v1'
pto_child_dag_id = f'ttec_process_each_pto_schedule_child_{instance}_v1'

disabled=True
