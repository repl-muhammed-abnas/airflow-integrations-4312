# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.time_export.config import *
from pwcglobal.time_export.mappers.location_dag_mapper_prod import location_dag_mapper_prod
from pwcglobal.time_export.mappers.timeofftype_chargecode_mapper import timeofftype_chargecode

instance = 'prod'
environment = 'production'

company_key = 'pwc'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'

master_dag_schedule = '*/30 * * * *'
send_long_running_job_email = True

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'

#pylint: disable=line-too-long
tenant_email = 'gbl_replicon_support_team@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

upload_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time"
log_filepath = "/PwCGBL_RepliconGlobal_PRD/PRD/Outbound/Time/_logs"
location_codes = list({x['code'].lower()
                      for x in location_dag_mapper_prod})
time_extract_mapper = f'pwc_time_extract_mapper_{instance}'

timeofftype_chargecode_mapper= timeofftype_chargecode

enable_uatmain_dag = False
if enable_uatmain_dag:
    nonuat_master_dag_schedule = '*/30 * * * *'
    uat_postfix = 'uat'
    non_uat_postfix = 'non-uat'
