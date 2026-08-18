# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import_v3.config import *
from pwcglobal.user_import_v3.mapper.general_qa import general_mapper
from pwcglobal.user_import_v3.mapper.toil_qa import toil_timeoff_mapper
from pwcglobal.user_import_v3.mapper.zerotime_permissionset_mapper_qa import zt_permission_mapper
from pwcglobal.user_import_v3.mapper.timesheet_policy import timesheet_policy_mapper
instance = 'PwCQA'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCQA'
replicon_conn_id = 'pwcqa-replicon-eu.userimport'
sftp_conn_id = "pwcglobalqa-MFT-STG-replicon"
keynamespace="FTE_Value"
input_filepath = "/PwCGBL_RepliconGlobal_STG/PeopleData/Inbound"
archive_filepath = "/PwCGBL_RepliconGlobal_STG/PeopleData/Archive"
log_filepath = "/PwCGBL_BOSALLogs_STG/QA/ToPwC"
secondary_log_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Inbound/Staff/PMDM/_logs"

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_secondary_upload_required = True

general_mapper=general_mapper
toil_timeoff_mapper=toil_timeoff_mapper
zt_permission_mapper=zt_permission_mapper
timesheet_policy_mapper=timesheet_policy_mapper
