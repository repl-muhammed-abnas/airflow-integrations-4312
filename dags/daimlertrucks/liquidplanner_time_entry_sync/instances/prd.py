# pylint: disable=wildcard-import unused-wildcard-import
from daimlertrucks.liquidplanner_time_entry_sync.config import *

instance = 'prod'
environment = "production"

company_key = "daimlertrucks"

sftp_conn_id = "sftp_daimlertrucks_540697"
replicon_conn_id = "daimlertrucks_replicon_replicon"

input_filepath = "/Production/LiquidPlanner"
archive_filepath = "/Production/LiquidPlanner/Archive"

archive_logs_filepath = "/Production/LiquidPlanner/Archive/Logs"
successfull_records_filepath = "/Production/LiquidPlanner/SuccessfullRecords"
rejected_records_filepath = "/Production/LiquidPlanner/RejectedRecords"

tenant_email = "Replicon-Support@daimlertruck.com,dtna-eng-timewiz@daimlertruck.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task_var_name = f'daimlertrucks_liquidplanner_time_entry_sync_{instance}_can_run_batch_task'
