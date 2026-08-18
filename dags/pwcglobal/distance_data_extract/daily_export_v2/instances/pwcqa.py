# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.distance_data_extract.daily_export_v2.config import *

region = 'eu-central-1'
environment = 'pre-production'

instance = 'pwcqa'
version = '_v2'

company_key = 'PwCQA'
replicon_conn_id = 'pwcqa-replicon-eu.automation'

sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'

output_file_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/S4/NL/distance/"
alternate_file_path = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/S4/NLMetis/"
log_file_path = '/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/_logs/'

is_upload_file_to_different_path_required = True

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

report_name = "Distance Traveled Report - Replicon Testing"
# pylint: disable=line-too-long
column_order = "TransactionDate,TimeEntryID,PartyID,ResourceGrade,LegalEntityPartyID,WorkdayID,TimesheetStartDate,TimesheetEndDate,Mileage,ChargeCode,WorkItemType,Comments,Distance,DistanceCarotherfuel,DistancePublictransport,DistanceCar100%electric,DistanceCarpetrol,DistanceCar(plugin)hybrid,DistanceCardiesel,Distance(e-)Bikeorwalking,DistanceMotorbikepetrol,DistanceMotorbikeelectric,Distancescooterpetrol,Distancescooterelectric,TotalDistance"

main_dag_id = f"pwcglobal_distance_data_extract_daily_report_for_netherlands_{instance}{version}"

disabled=True
