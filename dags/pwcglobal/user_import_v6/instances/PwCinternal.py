# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from pwcglobal.user_import_v6.config import *
from pwcglobal.user_import_v6.mapper.general_qa import general_mapper
from pwcglobal.user_import_v6.mapper.toil_qa import toil_timeoff_mapper
from pwcglobal.user_import_v6.mapper.zerotime_permissionset_mapper_qa import zt_permission_mapper
from pwcglobal.user_import_v6.mapper.timesheet_policy import timesheet_policy_mapper

instance = 'PwCinternal'
environment = 'pre-production'

company_key = 'PwCinternal'

replicon_conn_id = 'pwcinternal_replicon_eu.automation'
sftp_conn_id = 'sftp_internal_useast2'
secondary_sftp_conn_id = "sftp_internal_useast2"

keynamespace = "FTE_Value"

input_filepath = "/PWC/PwCInternal/user_Import/input"
archive_filepath = "/PWC/PwCInternal/user_Import/archive"
log_filepath = "/PWC/PwCInternal/user_Import/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_secondary_upload_required = False

general_mapper = general_mapper
toil_timeoff_mapper = toil_timeoff_mapper
zt_permission_mapper = zt_permission_mapper
timesheet_policy_mapper = timesheet_policy_mapper

version = "v6"

master_dag_id = f'pwcglobal_user_import_master_{instance}_{version}'

location_dag_id = f'pwcglobal_user_import_location_child_{instance}_{version}'
schedule_dag_id = f'pwcglobal_user_import_schedule_child_{instance}_{version}'
supervisor_dag_id = f'pwcglobal_user_import_supervisor_child_{instance}_{version}'

process_user_dag_id = f'pwcglobal_user_import_user_child_{instance}_{version}'
user_add_dag_id = f'pwcglobal_user_import_add_user_child_{instance}_{version}'
user_update_dag_id = f'pwcglobal_user_import_update_user_child_{instance}_{version}'

timesheet_punch_entry_policy_update_dag_id = f'pwcglobal_user_import_timesheet_punch_entry_policy_update_child_{instance}_{version}'
