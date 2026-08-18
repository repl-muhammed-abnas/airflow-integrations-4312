# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absense_data_pre_population.config import *
from pwcglobal.absense_data_pre_population.mappers.worktype_mapper_v1 import worktype

instance = 'qaafmig'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcqaafmig'
replicon_conn_id = 'pwcqaafmig-replicon-eu.automation'

bearer_token_var = f'pwc_webhook_absense_data_population_{instance}_secret'

sftp_conn_id = 'sftp_pwc_absense_data_population'

log_filepath = "/PwCQAafmig/Absense_Data_Population/logs"

can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'
WORKTYPE_MAPPER = worktype

master_dag_id = f'pwc_timesheetprepopulation_master_{instance}'

disabled = True
