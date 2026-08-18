# pylint: disable=wildcard-import unused-wildcard-import
from ttecholdingsinc.schedule_creation.config import *

region = 'us-east-1'
instance = 'uat'
environment = 'pre-production'
company_key = 'TTECHOLDINGSINCTRIAL01'

replicon_conn_id = 'ttecholdingsinctrial01_replicon_admin'
sftp_conn_id = 'sftp_ttecholdingsinctrial01_547658'

input_filepath = '/Trial/Schedule data/Input'
archive_filepath = '/Trial/Schedule data/Archive'
log_filepath = '/Trial/Schedule data/Log'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name =  f'ttec_shift_schedule_import_{instance}_can_run_batch_task'

create_schedule_dag_id = f'ttec_schedule_creation_child_{instance}'
shift_child_dag_id = f'ttec_process_each_shift_schedule_child_{instance}'
pto_child_dag_id = f'ttec_process_each_pto_schedule_child_{instance}'

disabled=True
