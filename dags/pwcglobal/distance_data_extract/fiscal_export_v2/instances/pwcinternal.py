# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.fiscal_export_v2.config import *

region = 'eu-central-1'
environment = 'pre-production'

instance = 'pwcinternal'
version = '_v2'

company_key = 'pwcinternal'
replicon_conn_id = 'replicon-pwcinternal'

sftp_conn_id = 'pwcinternal_sftp_airflowmig_eucentral'

output_file_path = '/pwcinternaltest/distance_data_extraxct/exportfile/'
alternate_file_path = "/pwcinternaltest/distance_data_extraxct/alternatepath/"

is_upload_file_to_different_path_required = True

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

report_name = "Distance Traveled Report-NLD (Automation)"
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkdayID,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Comments,Distance,DistanceCarotherfuel,DistancePublictransport,DistanceCar100%electric,DistanceCarpetrol,DistanceCar(plugin)hybrid,DistanceCardiesel,Distance(e-)Bikeorwalking,DistanceMotorbikepetrol,DistanceMotorbikeelectric,Distancescooterpetrol,Distancescooterelectric,TotalDistance"

main_dag_id = f"pwcglobal_previous_fiscal_year_distance_data_extract_report_for_netherlands_{instance}{version}"
upload_file_child_dag_id = f"pwcglobal_process_previous_fiscal_year_extract_upload_file_child_dag_{instance}{version}"

disabled=True
