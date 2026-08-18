# pylint: disable=wildcard-import unused-wildcard-import
from darkmattertechnologiesllc.user_sync.config import *
instance = 'uat'
company_key = 'DarkMatterTechnologiesLLCtrial01'

replicon_conn_id = 'darkmattertechnologiesllctrial01_replicon.admin'
sftp_conn_id = 'sftp_darkmattertechnologiesllc_admin'

pgp_conn_id = 'pgp_darkmattertechnologiesllc_userimport'

leave_status = ['On Leave', 'Maternity', 'Leave of Absence']
input_filepath = '/Trial/Import/User Sync/Input'
input_filepath_master = '/Trial/Import/User Sync/Processing/'
log_filepath = '/Trial/Import/User Sync/Log/'
archive_filepath = '/Trial/Import/User Sync/Archive/'

to_email = "Operations@dmatter.com,perseusworkdayhrsupport@csiperseus.com"
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

update_user_status_main_dagid = f"darkmattertechnologiesllctrial01_usersync_user_status_update_master_{instance}"

can_run_batch_task_var_name = f'darkmattertechnologiesllctrial01_usersync_{instance}_can_run_batch_task'

disabled=True
