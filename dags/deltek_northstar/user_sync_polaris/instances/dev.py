# pylint: disable=wildcard-import unused-wildcard-import
from deltek_northstar.user_sync_polaris.config import *
from deltek_northstar.user_sync_polaris.mapper.polaris_roles_mapper import polaris_roles_mapper
from deltek_northstar.user_sync_polaris.mapper.timezone_mapper import timezone_mapper
from deltek_northstar.user_sync_polaris.mapper.timeoff_type_mapper import timeoff_type_mapper
from deltek_northstar.user_sync_polaris.mapper.timesheet_period_mapper import timesheet_period_mapper

environment = 'pre-production'
instance = "dev"

company_key = 'DeltekDev'
sftp_conn_id = "sftp_internal"
replicon_conn_id = f'deltek_costpoint_polaris_{instance}'
deltek_cospoint_conn_id = 'deltek_costpoint_cp_basic_polaris'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag = f'deltek_costpoint_user_sync_master_{instance}'
process_users = f'deltek_costpoint_user_sync_process_each_user_child_{instance}'
processs_supervisor = f'deltek_costpoint_user_sync_process_pending_supervisor_child_{instance}'
process_new_users = f'deltek_costpoint_user_sync_process_new_users_child_{instance}'
process_update_users = f'deltek_costpoint_user_sync_process_update_users_child_{instance}'
process_log_generation = f'deltek_costpoint_user_sync_process_log_generation_child_{instance}'
process_disable_users = f'deltek_costpoint_user_sync_process_disable_users_child_{instance}'

can_run_batch_task = f'deltek_costpoint_user_sync_can_run_batch_task_{instance}'
can_use_conf_payload_var_name = f'deltek_costpoint_user_sync_can_use_conf_payload_var_{instance}'
last_run_date_var_name = f'deltek_costpoint_user_sync_last_run_date_{instance}'

log_filepath = '/shivam/cospoint/hris/logs'

POLARIS_ROLES_MAPPER = polaris_roles_mapper
TIMEZONE_MAPPER = timezone_mapper
TIMEOFF_TYPE_MAPPER = timeoff_type_mapper
TIMESHEET_PERIOD_MAPPER = timesheet_period_mapper
