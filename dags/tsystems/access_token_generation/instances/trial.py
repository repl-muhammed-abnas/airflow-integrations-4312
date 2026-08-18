# pylint: disable=wildcard-import unused-wildcard-import
from tsystems.access_token_generation.config import *

instance = "trial"
environment = "pre-production"
company_key = "TsystemsSB"

replicon_conn_id = 'tsystems_replicon_replicon.admin'
http_conn_id = f'http_tsystemssb_caiman_auth_access_token_{instance}'

master_dag_id = f"tsystems_caiman_access_token_generation_master_{instance}"

client_id_secret_variable_name = f"tsystems_caiman_client_id_secret_variable_{instance}"
token_var = f"tsystems_caiman_access_token_variable_{instance}"
