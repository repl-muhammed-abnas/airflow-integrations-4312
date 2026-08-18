# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from pwcglobal.user_import_v5.config import *
from pwcglobal.user_import_v5.mapper.general import general_mapper
from pwcglobal.user_import_v5.mapper.toil import toil_timeoff_mapper
from pwcglobal.user_import_v5.mapper.zerotime_permissionset_mapper import zt_permission_mapper
from pwcglobal.user_import_v5.mapper.timesheet_policy import timesheet_policy_mapper

instance = 'PwC'
environment = 'production'

company_key = 'PwC'

replicon_conn_id = 'pwcglobal-replicon-eu.userimport'
sftp_conn_id = "pwcglobal-MFT-PRD-replicon"
secondary_sftp_conn_id = "sftp_pwc_userimport"

keynamespace = "FTE_Value"

input_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM"
archive_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM/_archive"
log_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM/_logs"

tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_secondary_upload_required = False

general_mapper = general_mapper
toil_timeoff_mapper = toil_timeoff_mapper
zt_permission_mapper = zt_permission_mapper
timesheet_policy_mapper = timesheet_policy_mapper

user_dag_max_active_runs = 20
supervisor_dag_max_active_runs = 20

version = "v5"

master_dag_id = f'pwcglobal_user_import_master_{instance}_{version}'

location_dag_id = f'pwcglobal_user_import_location_child_{instance}_{version}'
schedule_dag_id = f'pwcglobal_user_import_schedule_child_{instance}_{version}'
supervisor_dag_id = f'pwcglobal_user_import_supervisor_child_{instance}_{version}'

process_user_dag_id = f'pwcglobal_user_import_user_child_{instance}_{version}'
user_add_dag_id = f'pwcglobal_user_import_add_user_child_{instance}_{version}'
user_update_dag_id = f'pwcglobal_user_import_update_user_child_{instance}_{version}'

timesheet_punch_entry_policy_update_dag_id = f'pwcglobal_user_import_timesheet_punch_entry_policy_update_child_{instance}_{version}'
