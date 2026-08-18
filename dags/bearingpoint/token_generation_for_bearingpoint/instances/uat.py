# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.token_generation_for_bearingpoint.config import *

instance = "uat"
environment = "pre-production"
company_key = "bearingpointsandbox"

replicon_conn_id = "bearingpointsandbox_replicon_admin"
http_conn_id = f'bearingpointsandbox_oauth2_http_conn_{instance}'

master_dag = f"bearingpoint_token_generation_master_{instance}"

client_id_secret_variable_name = f"bearingpoint_client_id_secret_variable_{instance}" # bearingpoint_client_id_secret_variable_uat
token_var = f"bearingpoint_token_variable_{instance}" #bearingpoint_token_variable_uat
