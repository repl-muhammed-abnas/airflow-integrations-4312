# pylint: disable=wildcard-import unused-wildcard-import
from crl.webhook_endpoints.project_import.config import *

instance = "qa"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriestrial01"
replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_riteam"
sftp_conn_id = 'rsftp-useast_for_testing'

input_filepath = '/crl/project/Archive/'

master_dag_id = f"crl_project_import_master_{instance}_v1"
process_payload_dagid = f'crl_project_import_process_payload_child_{instance}_v1'

crl_project_import_bearer_token_var = f"crl_project_import_bearer_token_variable_{instance}"

disabled=True
