# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from pwcglobal.user_import_v5.config import *
from pwcglobal.user_import_v5.mapper.general_qa import general_mapper
from pwcglobal.user_import_v5.mapper.toil_dev import toil_timeoff_mapper
from pwcglobal.user_import_v5.mapper.zerotime_permissionset_mapper_qa import zt_permission_mapper
from pwcglobal.user_import_v5.mapper.timesheet_policy import timesheet_policy_mapper

instance = 'PwCDev'
environment = 'pre-production'

company_key = 'PwCDev'

replicon_conn_id = 'pwcdev-replicon-eu.userimport'
sftp_conn_id = "pwcglobaldev-MFT-STG-replicon"
secondary_sftp_conn_id = "sftp_internal_useast2"

keynamespace = "FTE_Value"

input_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/PMDM"
archive_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/PMDM/_archive"
log_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/PMDM/_logs"

tenant_email = "PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_secondary_upload_required = False

general_mapper = general_mapper
toil_timeoff_mapper = toil_timeoff_mapper
zt_permission_mapper = zt_permission_mapper
timesheet_policy_mapper = timesheet_policy_mapper

version = "v5"

master_dag_id = f'pwcglobal_user_import_master_{instance}_{version}'

location_dag_id = f'pwcglobal_user_import_location_child_{instance}_{version}'
schedule_dag_id = f'pwcglobal_user_import_schedule_child_{instance}_{version}'
supervisor_dag_id = f'pwcglobal_user_import_supervisor_child_{instance}_{version}'

process_user_dag_id = f'pwcglobal_user_import_user_child_{instance}_{version}'
user_add_dag_id = f'pwcglobal_user_import_add_user_child_{instance}_{version}'
user_update_dag_id = f'pwcglobal_user_import_update_user_child_{instance}_{version}'

timesheet_punch_entry_policy_update_dag_id = f'pwcglobal_user_import_timesheet_punch_entry_policy_update_child_{instance}_{version}'
