# pylint: disable=wildcard-import unused-wildcard-import
from crl.webhook_endpoints.project_import.config import *

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_repliconint_projectimport"
sftp_conn_id = 'sftp_charlesriverlaboratories_603355'

input_filepath = '/Production/Inbound/Project Import/Archive/'

master_dag_id = f"crl_project_import_master_{instance}"
process_payload_dagid = f'crl_project_import_process_payload_child_{instance}_v3'

crl_project_import_bearer_token_var = f"crl_project_import_bearer_token_variable_{instance}"
