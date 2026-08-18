# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.germany.config import *

instance = "trial"
environment = "pre-production"

company_key = "moodysemeatrial03"

replicon_conn_id = "replicon_moodysemeatrial03_admin"
sftp_conn_id = "sftp_internal_useast2"

input_filepath = "moodys/User Sync/Processing/Germany"
archive_filepath = "moodys/User Sync/Archive"
log_filepath = "moodys/User Sync/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'moodys_user_sync_germany_master_{instance}'
process_users_dagid = f'moodys_user_sync_germany_child_process_users_{instance}'
process_log_generation_dagid = f'moodys_user_sync_germany_child_process_log_generation_{instance}'
process_new_users_dagid = f'moodys_user_sync_germany_child_process_new_users_{instance}'
process_update_users_dagid = f'moodys_user_sync_germany_child_process_update_users_{instance}'
processs_supervisor_dag_id = f'moodys_user_sync_germany_child_process_supervisors_{instance}'
process_groups_dag_id = f'moodys_user_sync_germany_child_process_groups_{instance}'
process_new_divisions_dagid = f'moodys_user_sync_germany_child_process_divisions_{instance}'

can_run_batch_task_var_name = f'moodys_user_sync_run_batch_task_{instance}'
