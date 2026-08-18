# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absense_data_pre_population.config import *
from pwcglobal.absense_data_pre_population.mappers.worktype_mapper_v1 import worktype

instance = 'trial'
company_key = 'pwcinternal'

replicon_conn_id = 'pwcinternal-replicon-eu.automation'

bearer_token_var = f'pwc_webhook_absense_data_population_{instance}_secret'

sftp_conn_id = 'eucentral_internal_sftp'

log_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/LogsInternal"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'



can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'

master_dag_id = f'pwc_timesheetprepopulation_master_{instance}'

WORKTYPE_MAPPER = worktype
disabled=True