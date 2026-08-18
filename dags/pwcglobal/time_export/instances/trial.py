# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.time_export.config import *
from pwcglobal.time_export.mappers.location_dag_mapper_trial import location_dag_mapper_trial
from pwcglobal.time_export.mappers.timeofftype_chargecode_mapper import timeofftype_chargecode

instance = 'trial'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

master_dag_schedule = '*/30 * * * *'
send_long_running_job_email = True

sftp_conn_id = 'sftp_pwc_timesheet_auto_submission'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

upload_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/OutboundInternal"
log_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/LogsInternal"
location_codes = list({x['code'].lower()
                      for x in location_dag_mapper_trial})
time_extract_mapper = f'pwc_time_extract_mapper_{instance}'

timeofftype_chargecode_mapper= timeofftype_chargecode

enable_uatmain_dag = False
if enable_uatmain_dag:
    nonuat_master_dag_schedule = '0 */1 * * *'
    uat_postfix = 'uat'
    non_uat_postfix = 'non-uat'
disabled = True
