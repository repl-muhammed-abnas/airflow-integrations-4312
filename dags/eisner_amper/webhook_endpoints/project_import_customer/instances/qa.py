# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.webhook_endpoints.project_import_customer.config import *

instance = 'qa'
environment = 'pre-production'

company_key = 'eisnerampertrial01'

replicon_conn_id = "eisnerampertrial01_replicon_radmin"

bearer_token_var = f'eisneramper_project_import_update_customer_secret_{instance}'

webhook_master_dagid = f'eisner_amper_project_import_update_customer_records_{instance}'
process_project_import_payload_dagid = f'eisner_amper_project_import_api_update_customer_records_master_{instance}'
