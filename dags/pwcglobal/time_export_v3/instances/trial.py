# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.time_export_v3.config import *
from pwcglobal.time_export_v3.mappers.location_dag_mapper_trial import location_dag_mapper_trial
from pwcglobal.time_export_v3.mappers.api_endpoints_details_trial import api_details
from pwcglobal.time_export_v3.mappers.timeofftype_chargecode_mapper_trial import timeofftype_chargecode

instance = 'trial'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

master_dag_schedule = '*/30 * * * *'
send_long_running_job_email = True

sftp_conn_id = 'Airflow_migration_SFTP_eucentral'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
api_failed_alert_email = '{{ var.value.dagrun_failure_alert_email }}'

upload_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/OutboundInternal"
log_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/LogsInternal"

location_codes = list({x['code'].lower()
                      for x in location_dag_mapper_trial})
time_extract_mapper = f'pwc_time_extract_mapper_{instance}_v1'
api_details_mapper = api_details
timeofftype_chargecode_mapper= timeofftype_chargecode

enable_uatmain_dag = False
if enable_uatmain_dag:
    nonuat_master_dag_schedule = '0 */1 * * *'
    uat_postfix = 'uat'
    non_uat_postfix = 'non-uat'

secondary_sftp = False
if secondary_sftp:
    secondary_sftp_conn_id = 'sftp_eucentral'
    secondary_upload_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/OutboundInternal"
    secondary_log_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/LogsInternal"

disabled = True
