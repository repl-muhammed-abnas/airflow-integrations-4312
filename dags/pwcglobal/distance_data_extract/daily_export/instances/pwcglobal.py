# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.daily_export.config import *

region = 'eu-central-1'
environment = 'production'
company_key = 'PwC'
instance = 'pwc'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'
sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'
output_file_path = '/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time/'
log_file_path = '/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time/_logs/'
alternate_file_path = ""
tenant_email = 'PWCGlobalLogs@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkDayId,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Distance"
query = """SELECT * FROM report_data_collection WHERE NULLIF(TransactionDate, '') IS NOT NULL AND Distance > 0"""
is_upload_file_to_different_path_required = False
disabled = True
