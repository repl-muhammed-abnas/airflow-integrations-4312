# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.daily_export_v1.config import *

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'pwcinternal'
instance = 'pwcinternal'
replicon_conn_id = 'replicon-pwcinternal'
sftp_conn_id = 'pwcinternal_sftp_airflowmig_eucentral'
report_name = "Distance Traveled Report-NLD (Automation)"
output_file_path = '/pwcinternaltest/distance_data_extraxct/exportfile/'
log_file_path = '/pwcinternaltest/distance_data_extraxct/logfile/'
alternate_file_path = "/pwcinternaltest/distance_data_extraxct/alternatepath/"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkDayId,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Comments,Distance"
query = """SELECT * FROM report_data_collection WHERE NULLIF(TransactionDate, '') IS NOT NULL AND Distance > 0"""
is_upload_file_to_different_path_required = True
disabled = True
