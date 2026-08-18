# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absense_data_pre_population.config import *
from pwcglobal.absense_data_pre_population.mappers.worktype_mapper_v1 import worktype

instance = 'dev'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'

bearer_token_var = f'pwc_webhook_absense_data_population_{instance}_secret'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

log_filepath = "/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeDev"

tenant_email = 'PWCGlobalLogs@deltek.com,us_replicondevextintegrationalerts@pwc.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'
WORKTYPE_MAPPER = worktype

master_dag_id = f'pwc_timesheetprepopulation_master_{instance}_old'

disabled=True

