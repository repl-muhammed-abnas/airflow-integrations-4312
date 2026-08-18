# pylint: disable=wildcard-import unused-wildcard-import
from strayeruniversity.user_sync_v3.config import *
instance = 'uat'
company_key = 'strayeruniversitytrial01'

schedule_interval = "0 20 * * *"

replicon_conn_id = 'strayeruniversitytrial01_repadmin'
sftp_conn_id = 'sftp_useast2_strayeruniversitytrial01_uat'
http_conn_id = "strayeruniversity_user_sync_workday_report_uat"

sftp_conn_id_internal = 'sftp_internal_useast'

user_name = "repadmin"

input_filepath = '/StrayerUniversity/Workdayusersync/userdata/input'
input_filepath_master = '/StrayerUniversity/Workdayusersync/userdata/Processing'
log_filepath = '/StrayerUniversity/Workdayusersync/userdata/Logs'
archive_filepath = '/StrayerUniversity/Workdayusersync/userdata/Archive'
reference_filepath = '/StrayerUniversity/Workdayusersync/userdata/Reference'

tenant_email = 'payroll@strayer.edu'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'strayeruniversity_usersync_{instance}_can_run_batch_task'
can_use_reference_file = 'Y'

disabled=True
