# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.webhooks_endpoint.absense_data_pre_population_webhook_v1.config import *
from pwcglobal.webhooks_endpoint.absense_data_pre_population_webhook_v1.mappers.locations_api_codes_mapper import locations_api_codes

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'pwc'
replicon_conn_id = 'pwcglobal-replicon-eu.automation'

location = 'austria'
api_unique_code = locations_api_codes[location]
version = 'v3'

webhook_dag_id = f'pwc_timesheetprepopulation_master_{instance}_{api_unique_code}'

bearer_token_var = f'pwc_webhook_absense_data_population_{api_unique_code}_{instance}_secret'

trigger_master_dag_id = f'pwc_timesheetprepopulation_main_child_{api_unique_code}_{instance}_{version}'