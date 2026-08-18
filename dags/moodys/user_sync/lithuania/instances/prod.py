# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.lithuania.config import *

instance = "production"
environment = "production"

company_key = "MoodysEMEA"

replicon_conn_id = "moodysemea_replicon_integrationuser"
sftp_conn_id = "sftp_moodysemea_654601"

input_filepath = "/MoodysEMEA/Prod/Usersync/Processing/Lithuania"
archive_filepath = "/MoodysEMEA/Prod/Usersync/Archive"
log_filepath = "/MoodysEMEA/Prod/Usersync/Logs"

# pylint: disable=line-too-long
tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'moodys_user_sync_lithuania_master_{instance}'
process_users_dagid = f'moodys_user_sync_lithuania_child_process_users_{instance}'
process_log_generation_dagid = f'moodys_user_sync_lithuania_child_process_log_generation_{instance}'
process_new_users_dagid = f'moodys_user_sync_lithuania_child_process_new_users_{instance}'
process_update_users_dagid = f'moodys_user_sync_lithuania_child_process_update_users_{instance}'
processs_supervisor_dag_id = f'moodys_user_sync_lithuania_child_process_supervisors_{instance}'
process_groups_dag_id = f'moodys_user_sync_lithuania_child_process_groups_{instance}'
process_new_divisions_dagid = f'moodys_user_sync_lithuania_child_process_divisions_{instance}'

can_run_batch_task_var_name = f'moodys_user_sync_run_batch_task_{instance}'
