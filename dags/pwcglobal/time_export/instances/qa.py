# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.time_export.config import *
from pwcglobal.time_export.mappers.location_dag_mapper_qa import location_dag_mapper_qa
from pwcglobal.time_export.mappers.timeofftype_chargecode_mapper import timeofftype_chargecode

instance = 'qa'

company_key = 'pwcqa'
replicon_conn_id = 'pwcqa-replicon-eu.automation'

master_dag_schedule = '*/30 * * * *'
send_long_running_job_email = False

sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'

#pylint: disable=line-too-long
tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

upload_filepath = "/PwCGBL_RepliconGlobal_STG/TimeData/OutboundQA"
log_filepath = "/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA"
location_codes = list({x['code'].lower()
                      for x in location_dag_mapper_qa})
time_extract_mapper = f'pwc_time_extract_mapper_{instance}'

timeofftype_chargecode_mapper= timeofftype_chargecode

enable_uatmain_dag = True
if enable_uatmain_dag:
    nonuat_master_dag_schedule = '0 */2 * * *'
    uat_postfix = 'uat'
    non_uat_postfix = 'non-uat'

secondary_sftp = True
if secondary_sftp:
    secondary_sftp_conn_id = 'pwcglobalqa-MFT-STG-replicon'
    secondary_upload_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time"
    secondary_log_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time/_logs"
disabled = True
