# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.time_export.config import *
from pwcglobal.time_export.mappers.location_dag_mapper_dev import location_dag_mapper_dev
from pwcglobal.time_export.mappers.timeofftype_chargecode_mapper import timeofftype_chargecode

instance = 'dev'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'

master_dag_schedule = '*/30 * * * *'
send_long_running_job_email = False

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

#pylint: disable=line-too-long
tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

upload_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time"
log_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time/_logs"
location_codes = list({x['code'].lower()
                      for x in location_dag_mapper_dev})
time_extract_mapper = f'pwc_time_extract_mapper_{instance}'

timeofftype_chargecode_mapper= timeofftype_chargecode

enable_uatmain_dag = True
if enable_uatmain_dag:
    nonuat_master_dag_schedule = '0 */3 * * *'
    uat_postfix = 'uat'
    non_uat_postfix = 'non-uat'

secondary_sftp = True
if secondary_sftp:
    secondary_sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'
    secondary_upload_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time/S4"
    secondary_log_filepath = "/PwCGBL_RepliconGlobal_STG/DEV/Outbound/Time/S4/_logs"
disabled = True
