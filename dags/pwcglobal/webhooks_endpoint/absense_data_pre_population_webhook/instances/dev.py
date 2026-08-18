# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.webhooks_endpoint.absense_data_pre_population_webhook.config import *


instance = 'dev'
region = 'eu-central-1'
environment = 'pre-production'
version = 'v3'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'

bearer_token_var = f'pwc_webhook_absense_data_population_{instance}_secret'

trigger_master_dag_id = f'pwc_timesheetprepopulation_main_child_000_{instance}_{version}'