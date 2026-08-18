# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.united_states.config import *

instance = "uat"
environment = "pre-production"

company_key = "moodysemeatrial02"

replicon_conn_id = "moodysemeatrial02_replicon_deepak"
sftp_conn_id = "sftp_moodysemeatrial02_654601"

input_filepath = "/MoodysEMEA/UAT/Usersync/Processing/UnitedStates"
archive_filepath = "/MoodysEMEA/UAT/Usersync/Archive"
log_filepath = "/MoodysEMEA/UAT/Usersync/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'moodys_user_sync_united_states_master_{instance}'
process_users_dagid = f'moodys_user_sync_united_states_child_process_users_{instance}'
process_log_generation_dagid = f'moodys_user_sync_united_states_child_process_log_generation_{instance}'
process_new_users_dagid = f'moodys_user_sync_united_states_child_process_new_users_{instance}'
process_update_users_dagid = f'moodys_user_sync_united_states_child_process_update_users_{instance}'
processs_supervisor_dag_id = f'moodys_user_sync_united_states_child_process_supervisors_{instance}'
process_groups_dag_id = f'moodys_user_sync_united_states_child_process_groups_{instance}'
process_new_divisions_dagid = f'moodys_user_sync_united_states_child_process_divisions_{instance}'

can_run_batch_task_var_name = f'moodys_user_sync_run_batch_task_{instance}'

disable=True

disabled=True
