from pwcglobal.absense_data_pre_population_v2.config import *
from pwcglobal.absense_data_pre_population_v2.mappers.worktype_mapper import worktype

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'pwc'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'

bearer_token_var = f'pwc_webhook_absense_data_population_{instance}_secret'

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'

log_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Time/_logs'

tenant_email = 'gbl_replicon_support_team@pwc.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'
WORKTYPE_MAPPER = worktype