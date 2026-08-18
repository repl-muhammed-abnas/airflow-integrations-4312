# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absense_data_pre_population_v3.config import *
from pwcglobal.absense_data_pre_population_v3.mappers.worktype_mapper_v1 import worktype
from pwcglobal.absense_data_pre_population_v3.mappers.locations_api_codes_mapper import locations_api_codes

instance = 'qa'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'pwcqa'
replicon_conn_id = 'pwcqa-replicon-eu.automation'

location = 'switzerland_liechtenstein'
api_unique_code = locations_api_codes[location]
version = 'v3'

master_dag_id = f'pwc_timesheetprepopulation_main_child_{api_unique_code}_{instance}_{version}'
child_dag_id = f'pwc_timesheetprepopulation_child_{api_unique_code}_{instance}'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

log_filepath = "/PwCGBL_RepliconGlobal_STG/TimeData/Logs/TimeQA"

tenant_email = 'PWCGlobalLogs@deltek.com,us_repliconqaextintegrationalerts@pwc.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


# copy the below lines to the respective instance config if secondary sftp needs to be set up
secondary_sftp = True  # set this to True to enable secondary_sftp
if secondary_sftp:
    secondary_sftp_conn_id = 'sftp_pwc_absense_data_population'
    secondary_log_filepath = '/PwCGBL_RepliconGlobal_STG/QA/Inbound/Time/_logs/'

can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'
WORKTYPE_MAPPER = worktype