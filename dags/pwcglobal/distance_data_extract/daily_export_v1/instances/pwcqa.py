# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.daily_export_v1.config import *

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'PwCQA'
instance = 'pwcqa'
replicon_conn_id = 'pwcqa-replicon-eu.automation'
sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'
report_name = "Distance Traveled Report - NLD (Automation)"
output_file_path = '/PwCGBL_RepliconGlobal_STG/TimeData/OutboundQA/'
log_file_path = '/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA/'
alternate_file_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/"
tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkDayId,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Comments,Distance"
query = """SELECT * FROM report_data_collection WHERE NULLIF(TransactionDate, '') IS NOT NULL AND Distance > 0"""
is_upload_file_to_different_path_required = True
