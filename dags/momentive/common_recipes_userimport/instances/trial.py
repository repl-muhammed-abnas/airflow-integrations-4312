from datetime import timedelta
from momentive.common_recipes_userimport.config import *

region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'momentiveafmig'

replicon_conn_id = 'momentiveafmig_replicon_replicon.admin'
sftp_conn_id = 'sftp_useast2'

schedule_interval = timedelta(seconds=60)

input_filepath_for_trial = '/Momentive/UserSync/OtherCountries/input'
log_filepath = '/Momentive/UserSync/OtherCountries/logs'
archive_filepath = '/Momentive/UserSync/OtherCountries/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'momentive_user_import_othercountries_can_run_batch_task_{instance}'

momentive_othercountries_user_sync_add_user_child_dag_id = f'momentive_othercountries_user_sync_add_user_child_{instance}'
momentive_othercountries_user_sync_update_user_child_dag_id = f'momentive_othercountries_user_sync_update_user_child_{instance}'
momentive_othercountries_user_sync_disable_user_child_dag_id = f'momentive_othercountries_user_sync_disable_user_child_{instance}'
momentive_othercountries_user_sync_supervisor_assignment_dag_id = f'momentive_othercountries_user_sync_supervisor_assignment_{instance}'
momentive_othercountries_user_sync_timeoff_new_user_child_dag_id = f'momentive_othercountries_user_sync_timeoff_new_user_child_{instance}'
momentive_othercountries_user_sync_timeoff_rehire_user_child_dag_id = f'momentive_othercountries_user_sync_timeoff_rehire_user_child_{instance}'
momentive_othercountries_user_sync_update_user_timeoff_assign_child_dag_id = f'momentive_othercountries_user_sync_update_user_timeoff_assign_child_{instance}'
momentive_othercountries_user_sync_zero_balance_timeoff_update_child_dag_id = f'momentive_othercountries_user_sync_zero_balance_timeoff_update_child_{instance}'
momentive_othercountries_user_sync_put_zero_balance_payout_child_dag_id = f'momentive_othercountries_user_sync_put_zero_balance_payout_child_{instance}'
momentive_othercountries_user_sync_policy_rehire_update_days_child_dag_id = f'momentive_othercountries_user_sync_policy_rehire_update_days_child_{instance}'

# STUB target: update_user_timeoff_assign fans out to this, but the source recipe is NOT
# yet ported to this folder (1362490 = BEL Time off policy update_rehire). Trigger point is
# wired; the DAG must be built. (UK 1435242 is ON HOLD and intentionally not wired.)
momentive_othercountries_user_sync_bel_policy_rehire_child_dag_id = f'momentive_othercountries_user_sync_bel_policy_rehire_child_{instance}'