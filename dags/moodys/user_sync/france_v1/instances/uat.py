# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.france_v1.config import *

instance = "uat"
environment = "pre-production"

company_key = "moodysemeatrial03"

replicon_conn_id = "replicon_moodysemeatrial03_admin"
sftp_conn_id = "sftp_moodysemeatrial02_654601"

input_filepath = "/MoodysEMEA/UAT/Usersync/Processing/France"
archive_filepath = "/MoodysEMEA/UAT/Usersync/Archive"
log_filepath = "/MoodysEMEA/UAT/Usersync/Logs"

tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "_v1"

master_dagid = f'moodys_user_sync_france_master_{instance}{version}'
process_users_dagid = f'moodys_user_sync_france_child_process_users_{instance}{version}'
process_log_generation_dagid = f'moodys_user_sync_france_child_process_log_generation_{instance}{version}'
process_new_users_dagid = f'moodys_user_sync_france_child_process_new_users_{instance}{version}'
process_update_users_dagid = f'moodys_user_sync_france_child_process_update_users_{instance}{version}'
processs_supervisor_dag_id = f'moodys_user_sync_france_child_process_supervisors_{instance}{version}'
process_groups_dag_id = f'moodys_user_sync_france_child_process_groups_{instance}{version}'
process_new_divisions_dagid = f'moodys_user_sync_france_child_process_divisions_{instance}{version}'

can_run_batch_task_var_name = f'moodys_user_sync_run_batch_task_{instance}'
