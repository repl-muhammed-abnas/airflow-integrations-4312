# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.calendar_export_v1.config import *

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'PwCDEV'
instance = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'
sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'
output_file_path = '/PwCGBL_RepliconGlobal_STG/DEV/TimeData/OutboundDEV/'
log_file_path = '/PwCGBL_RepliconGlobal_STG/DEV/TimeData/Logs/TimeDEV/'
alternate_file_path = "/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time/"
tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkDayId,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Comments,Distance"

disable=True

disabled=True
