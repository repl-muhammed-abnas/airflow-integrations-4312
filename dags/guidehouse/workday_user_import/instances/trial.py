# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.workday_user_import.config import *
from guidehouse.workday_user_import.mappers.timeoff_mapper import timeoff_mapper
from guidehouse.workday_user_import.mappers.workweek_mapper import workweek_mapper
from guidehouse.workday_user_import.mappers.timezone_mapper import timezone_mapper
from guidehouse.workday_user_import.mappers.user_sync_mapper import user_sync_mapper
from guidehouse.workday_user_import.mappers.holiday_entitlement_mapper import holiday_entitlement_mapper

region = "us-east-1"

# Instance identification
instance = 'trial'
company_key = 'GuideHouseIncSB2'
replicon_conn_id = 'guidehousesb2_replicon_repliconint'
pgp_conn_id = 'guidehousesb2_replicon_pgp_conn_inbound'

# SFTP configuration
sftp_conn_id = 'sftp_guidehousesb2_678659_uat'
input_filepath = '/SIT/Inbound/Workday/Input'
archive_filepath = '/SIT/Inbound/Workday/Archive'
log_filepath = '/SIT/Inbound/Workday/Logs'

# Email configuration
tenant_email = 'guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

version = "" # _v1, _v2, etc. 
postfix = f"{instance}{version}"

# DAG identifiers
master_dag = f'guidehouse_workday_user_import_master_{postfix}'
process_each_user = f'guidehouse_workday_user_import_process_users_child_{postfix}'
process_new_users = f'guidehouse_workday_user_import_process_new_users_child_{postfix}'
process_update_users = f'guidehouse_workday_user_import_process_update_users_child_{postfix}'
processs_supervisor = f'guidehouse_workday_user_import_processs_supervisor_child_{postfix}'
process_log_generation = f'guidehouse_workday_user_import_process_log_generation_child_{postfix}'

process_new_locations = f'guidehouse_workday_user_import_process_locations_child_{postfix}'
process_new_usertypes = f'guidehouse_workday_user_import_process_usertypes_child_{postfix}'
process_new_schedule = f'guidehouse_workday_user_import_process_new_schedule_child_{postfix}'
process_disable_users = f'guidehouse_workday_user_import_process_disable_users_child_{postfix}'
process_zero_timeoff_policies = f'guidehouse_workday_user_import_process_zero_timeoff_policies_child_{postfix}'

can_decrypt_file_var_name = f'guidehouse_workday_user_import_can_decrypt_file_{postfix}'
can_run_batch_task = f'guidehouse_workday_user_import_can_run_batch_task_{postfix}'

TIMEOFF_MAPPER = timeoff_mapper
WORKWEEK_MAPPER = workweek_mapper
TIMEZONE_MAPPER = timezone_mapper
USER_SYNC_MAPPER = user_sync_mapper
HOLIDAY_ENTITLEMENT_MAPPER = holiday_entitlement_mapper
