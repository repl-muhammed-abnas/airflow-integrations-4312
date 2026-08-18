# pylint: disable=wildcard-import unused-wildcard-import
from darkmattertechnologiesllc.user_sync_v1.config import *
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
reference_filepath = '/Trial/Import/User Sync/Reference/'

to_email = "Operations@dmatter.com,perseusworkdayhrsupport@csiperseus.com"
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

update_user_status_main_dagid = f"darkmattertechnologiesllctrial01_usersync_user_status_update_master_{instance}_v1"
add_location_child_dagid = f'darkmattertechnologiesllc_usersync_add_location_child_{instance}_v1'
add_user_child_dagid = f'darkmattertechnologiesllc_usersync_add_user_child_{instance}_v1'
main_dagid = f'darkmattertechnologiesllc_usersync_master_{instance}_v1'
pgp_file_decryption_main_dagid = f'darkmattertechnologiesllc_usersync_decrypting_files_{instance}_v1'
process_each_user_child_dagid = f'darkmattertechnologiesllc_usersync_process_each_user_child_{instance}_v1'
process_group_child_dagid = f'darkmattertechnologiesllc_usersync_process_groups_child_{instance}_v1'
supervisor_assignment_child_dagid = f'darkmattertechnologiesllc_usersync_update_supervisor_child_{instance}_v1'
update_user_child_dagid = f'darkmattertechnologiesllc_usersync_update_user_child_{instance}_v1'
assign_timeoff_newuser_child_dagid = f'darkmattertechnologiesllc_usersync_assign_timeoff_newuser_child_{instance}_v1'

can_run_batch_task_var_name = f'darkmattertechnologiesllctrial01_usersync_{instance}_v1_can_run_batch_task'
can_use_reference_file = f'darkmattertechnologiesllctrial01_usersync_can_use_reference_file_{instance}_v1'

disabled=True
