# pylint: disable=wildcard-import unused-wildcard-import
from darkmattertechnologiesllc.user_sync.config import *
region = 'us-east-1'
environment = 'production'
instance = 'prod'
company_key = 'DarkMatterTechnologiesLLC'

replicon_conn_id = 'darkmattertechnologiesllc_replicon.admin'
sftp_conn_id = 'sftp_darkmattertechnologiesllc_admin'

pgp_conn_id = 'pgp_darkmattertechnologiesllc_userimport'

leave_status = ['On Leave', 'Maternity', 'Leave of Absence']
input_filepath = '/Production/Import/User Sync/Input'
input_filepath_master = '/Production/Import/User Sync/Processing/'
log_filepath = '/Production/Import/User Sync/Log/'
archive_filepath = '/Production/Import/User Sync/Archive/'

to_email = "Operations@dmatter.com,perseusworkdayhrsupport@csiperseus.com"
bcc_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

update_user_status_main_dagid = f"darkmattertechnologiesllc_usersync_user_status_update_master_{instance}"

can_run_batch_task_var_name = f'darkmattertechnologiesllc_usersync_{instance}_can_run_batch_task'
