# pylint: disable=wildcard-import unused-wildcard-import
from strayeruniversity.user_sync_v3.config import *
instance = 'prod'
environment = 'production'

company_key = 'StrayerUniversity'

schedule_interval = "0 20 * * *"

replicon_conn_id = 'strayeruniversity_replicon_repadmin'
sftp_conn_id = 'sftp_strayeruniversity_550029'
http_conn_id = "strayeruniversity_user_sync_workday_report_prod_http"

sftp_conn_id_internal = 'sftp_internal_useast2'

user_name = "repadmin"

input_filepath = '/Workdayusersync/userdata/input'
input_filepath_master = '/StrayerUniversity/Workdayusersync/userdata/Processing'
log_filepath = '/StrayerUniversity/Workdayusersync/userdata/Logs'
archive_filepath = '/Workdayusersync/userdata/archives'
reference_filepath = '/Workdayusersync/userdata/reference'

tenant_email = 'payroll@strategiced.com,payroll@strayer.edu'
bcc_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'strayeruniversity_usersync_{instance}_can_run_batch_task'
can_use_reference_file = 'Y'
