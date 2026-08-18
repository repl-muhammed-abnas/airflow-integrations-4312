# pylint: disable=wildcard-import unused-wildcard-import
from wikwemikongboard.timeoffbalancetransfer.config import *

instance = 'trial'


company_key = 'WikwemikongBoardafmig'
replicon_conn_id = 'WikwemikongBoardafmig_replicon_MAIABENS'
sftp_conn_id = 'sftp_useast2'

log_filepath = '/timeoffbalanceimport'

max_active_dag_runs = 1

max_active_runs_batch_child = 20
trigger_parallel_dagrun_count = 20

time_zone = 'PST8PDT'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

get_timeoff_child_dag_id = f"wikwemikongboard_timeoffbalancetransfer_get_timeoff_child_{instance}"
main_dag_id = f"wikwemikongboard_timeoffbalancetransfer_master_dag_{instance}"
timeoff_child_dag_id = f"wikwemikongboard_timeoffbalancetransfer_timeoff_transfer_child_{instance}"
process_log_generation = f"wikwemikongboard_timeoffbalancetransfer_process_log_generation_child_{instance}"

can_run_batch_task_child = f'wikwemikongboard_timeofftransfer_{instance}_can_run_batch_task'
