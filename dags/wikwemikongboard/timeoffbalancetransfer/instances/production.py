# pylint: disable=wildcard-import unused-wildcard-import
from wikwemikongboard.timeoffbalancetransfer.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'


company_key = 'WikwemikongBoard'
replicon_conn_id = 'WikwemikongBoard_replicon_tdowdall'
sftp_conn_id = 'sftp_Integration_useast_prod'

log_filepath = '/WikwemikongBoard/timeoffbalanceimport'

max_active_dag_runs = 1

max_active_runs_batch_child = 20
trigger_parallel_dagrun_count = 20

time_zone = 'America/Los_Angeles'

tenant_email = 'maiabens@wbe-education.ca'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

get_timeoff_child_dag_id = f"wikwemikongboard_timeoffbalancetransfer_get_timeoff_child_{instance}"
main_dag_id = f"wikwemikongboard_timeoffbalancetransfer_master_dag_{instance}"
timeoff_child_dag_id = f"wikwemikongboard_timeoffbalancetransfer_timeoff_transfer_child_{instance}"
process_log_generation = f"wikwemikongboard_timeoffbalancetransfer_process_log_generation_child_{instance}"

can_run_batch_task_child = f'wikwemikongboard_timeofftransfer_{instance}_can_run_batch_task'
