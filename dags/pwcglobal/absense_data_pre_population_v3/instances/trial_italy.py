# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absense_data_pre_population_v3.config import *
from pwcglobal.absense_data_pre_population_v3.mappers.worktype_mapper_v1 import worktype
from pwcglobal.absense_data_pre_population_v3.mappers.locations_api_codes_mapper import locations_api_codes

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

location = 'italy'
api_unique_code = locations_api_codes[location]
version = 'v3'

master_dag_id = f'pwc_timesheetprepopulation_main_child_{api_unique_code}_{instance}_{version}'
child_dag_id = f'pwc_timesheetprepopulation_child_{api_unique_code}_{instance}'

sftp_conn_id = 'sftp_useast2'

log_filepath = '/PwCGlobal/Absense_Data_Population/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'
WORKTYPE_MAPPER = worktype
disabled=True
