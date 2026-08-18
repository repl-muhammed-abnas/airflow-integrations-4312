# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.webhook_endpoints.project_import_customer_add_api.config import *

instance = 'production'
environment = 'production'

company_key = 'EisnerAmper'

replicon_conn_id = "eisneramper_repliconint.projectimport"

bearer_token_var = f'eisneramper_project_import_customer_add_api_secret_{instance}'

webhook_master_dagid = f'eisner_amper_project_import_add_customer_records_{instance}'
process_project_import_payload_dagid = f'eisner_amper_project_import_api_add_customer_records_master_{instance}'
