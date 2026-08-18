# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import_v3.config import *
from pwcglobal.user_import_v3.mapper.general import general_mapper
from pwcglobal.user_import_v3.mapper.toil import toil_timeoff_mapper
from pwcglobal.user_import_v3.mapper.zerotime_permissionset_mapper import zt_permission_mapper
from pwcglobal.user_import_v3.mapper.timesheet_policy import timesheet_policy_mapper

instance = 'PwC'
region = 'eu-central-1'
environment = 'production'

company_key = 'PwC'
replicon_conn_id = 'pwcglobal-replicon-eu.userimport'
sftp_conn_id = "pwcglobal-MFT-PRD-replicon"
keynamespace="FTE_Value"
input_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM"
archive_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM/_archive"
log_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/PMDM/_logs"

tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_secondary_upload_required = False

user_dag_max_active_runs = 20
supervisor_dag_max_active_runs = 20

general_mapper=general_mapper
toil_timeoff_mapper=toil_timeoff_mapper
zt_permission_mapper=zt_permission_mapper
timesheet_policy_mapper=timesheet_policy_mapper