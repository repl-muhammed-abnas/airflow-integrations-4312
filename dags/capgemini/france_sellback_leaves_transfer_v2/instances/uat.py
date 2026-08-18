# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.france_sellback_leaves_transfer_v2.config import *
from capgemini.france_sellback_leaves_transfer_v2.mappers.transfer_timeoff_types import timeoff_types

instance = 'uat'

environment = 'pre-production'

company_key = 'capgeminiuat'

replicon_conn_id = 'capgeminiuat_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'

log_filepath = "/Internal/France_RTT_Sellback_Leaves_Transfer/Logs"

max_active_runs = 1
max_active_child_runs = 5
assign_policy_parallel_count = 10
execution_timeout_days = 14

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_file_prefix = "UAT"

timeoff_types_mapper = timeoff_types

can_run_batch_task_var_name = f'capgemini_france_sellback_leaves_transfer_can_run_batch_task_{instance}_v2'
master_dagid = f'capgemini_france_sellback_leaves_transfer_master_{instance}_v2'
assign_policy_child_dagid = f'capgemini_france_sellback_leaves_transfer_assign_policy_to_user_child_{instance}_v2'
