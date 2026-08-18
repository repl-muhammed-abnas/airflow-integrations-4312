# pylint: disable=wildcard-import unused-wildcard-import
from ttecholdingsinc.schedule_creation.config import *

region = 'us-east-1'
instance = 'prod'
environment = 'production'
company_key = 'TTECHoldingsInc'

replicon_conn_id = 'ttecholdingsinc_replicon_admin'
sftp_conn_id = 'sftp_ttecholdingsinc_547658'

input_filepath = '/Production/Schedule Data/Input'
archive_filepath = '/Production/Schedule Data/Archive'
log_filepath = '/Production/Schedule Data/Log'

tenant_email = 'KronosTechnical@ttec.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task_var_name =  f'ttec_shift_schedule_import_{instance}_can_run_batch_task'

create_schedule_dag_id = f'ttec_schedule_creation_child_{instance}'
shift_child_dag_id = f'ttec_process_each_shift_schedule_child_{instance}'
pto_child_dag_id = f'ttec_process_each_pto_schedule_child_{instance}'
