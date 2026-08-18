# pylint: disable=wildcard-import unused-wildcard-import
from strayeruniversity.user_sync_v4.config import *
from strayeruniversity.user_sync_v4.mappers.strayer_payrule_mapper import payrule_mapper
from strayeruniversity.user_sync_v4.mappers.strayer_timezone_mapper import timezone_mapper
from strayeruniversity.user_sync_v4.mappers.strayer_schedule_mapper import schedule_mapper
from strayeruniversity.user_sync_v4.mappers.strayer_dynamic_timeoff_mapper import dynamic_timeoff_mapper
from strayeruniversity.user_sync_v4.mappers.strayer_static_timeoff_mapper import static_timeoff_mapper

instance = 'uat'
company_key = 'strayeruniversitytrial01'

schedule_interval = "0 20 * * *"

replicon_conn_id = 'strayeruniversitytrial01_repadmin'
sftp_conn_id = 'sftp_useast2_strayeruniversitytrial01_uat'
http_conn_id = "strayeruniversity_user_sync_workday_report_uat"

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
can_use_reference_file = f'strayeruniversity_usersync_{instance}_can_use_reference_file'

version = 'v4'

master_dag_id = f'strayeruniversity_usersync_master_{instance}_{version}'

child_proecss_each_user_dag_id = f'strayeruniversity_usersync_proecss_each_user_child_{instance}_{version}'

child_update_user_dag_id = f'strayeruniversity_usersync_update_user_child_{instance}_{version}'
child_add_user_dag_id = f'strayeruniversity_usersync_add_user_child_{instance}_{version}'

child_update_supervisor_dag_id = f'strayeruniversity_usersync_update_supervisor_child_{instance}_{version}'
child_assign_substitute_user_dag_id = f'strayeruniversity_usersync_assign_substitute_user_child_{instance}_{version}'

child_disable_user_dag_id = f'strayeruniversity_usersync_disable_user_child_{instance}_{version}'
child_remove_future_time_off_bookings_dag_id = f'strayeruniversity_usersync_remove_future_time_off_bookings_child_{instance}_{version}'
child_assign_0_balance_timeoff_dag_id = f'strayeruniversity_usersync_assign_0_balance_timeoff_child_{instance}_{version}'

child_process_customfield_for_dropdown_dag_id = f'strayeruniversity_usersync_process_customfield_for_dropdown_child_{instance}_{version}'
child_managementlevel_customfield_check_dag_id = f'strayeruniversity_managementlevel_customfield_check_child_{instance}_{version}'

PAYRULE_MAPPER = payrule_mapper
TIMEZONE_MAPPER = timezone_mapper
SCHEDULE_MAPPER = schedule_mapper
DYNAMIC_TIMEOFF_MAPPER = dynamic_timeoff_mapper
STATIC_TIMEOFF_MAPPER = static_timeoff_mapper

disabled=True
