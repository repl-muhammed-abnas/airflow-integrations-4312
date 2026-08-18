# pylint: disable=wildcard-import unused-wildcard-import
from strayeruniversity.user_sync_v3.config import *
from datetime import timedelta

instance = 'trial'
company_key = 'strayeruniversitytrial01'

schedule_interval = timedelta(seconds=60)

replicon_conn_id = 'strayeruniversitytrial01_repadmin'
sftp_conn_id = 'sftp_useast2'
http_conn_id = "strayeruniversity_user_sync_workday_report_uat"

sftp_conn_id_internal = 'sftp_internal_useast'

user_name = "repadmin"

input_filepath = '/StrayerUniversityLocalTrial/Workdayusersync/userdata/input'
input_filepath_master = '/StrayerUniversityLocalTrial/Workdayusersync/userdata/Processing'
log_filepath = '/StrayerUniversityLocalTrial/Workdayusersync/userdata/Logs'
archive_filepath = '/StrayerUniversityLocalTrial/Workdayusersync/userdata/Archive'
reference_filepath = '/StrayerUniversityLocalTrial/Workdayusersync/userdata/Reference'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'strayeruniversity_usersync_{instance}_can_run_batch_task'
can_use_reference_file = 'Y'

disabled=True
