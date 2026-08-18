# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.calendar_export_v1.config import *

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'pwcinternal'
instance = 'pwcinternal'
replicon_conn_id = 'replicon-pwcinternal'
report_name = "Distance Traveled Report-NLD (Automation)"
sftp_conn_id = 'pwcinternal_sftp_airflowmig_eucentral'
output_file_path = '/pwcinternaltest/distance_data_extraxct/exportfile/'
log_file_path = '/pwcinternaltest/distance_data_extraxct/logfile/'
alternate_file_path = "/pwcinternaltest/distance_data_extraxct/alternatepath/"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkDayId,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Comments,Distance"
disabled = True
