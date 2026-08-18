# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.absense_data_pre_population_v1.config import *
from pwcglobal.absense_data_pre_population_v1.mappers.worktype_mapper_v1 import worktype

instance = 'trial'
company_key = 'pwcinternal'

can_redirect_to_workato_var_name = f'pwc_timesheetprepopulation_{instance}_redirect_to_workato'
workato_api_endpoint = f'pwc_timesheetprepopulation_{instance}_workato_endpoint'
workato_api_token_var_name = f'pwc_timesheetprepopulation_{instance}_workato_api_token'

can_run_batch_task_var_name = f'pwc_timesheetprepopulation_{instance}_can_run_batch_task'

WORKTYPE_MAPPER = worktype
disabled=True