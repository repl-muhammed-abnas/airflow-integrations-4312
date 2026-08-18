# pylint: disable=wildcard-import unused-wildcard-import
from crl.webhook_endpoints.project_import.config import *

instance = "trial"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"
replicon_conn_id = "charlesriverlaboratoriessandbox_replicon_rit"
sftp_conn_id = 'sftp_crl_603355'

input_filepath = '/Test/Inbound/Project Import/Archive/'

master_dag_id = f"crl_project_import_master_{instance}"
process_payload_dagid = f'crl_project_import_process_payload_child_{instance}_v3'

crl_project_import_bearer_token_var = f"crl_project_import_bearer_token_variable_{instance}"
