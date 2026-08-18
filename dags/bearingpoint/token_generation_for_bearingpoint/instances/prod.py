# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.token_generation_for_bearingpoint.config import *

instance = "prod"
environment = "production"
company_key = "bearingpointgmbh"

replicon_conn_id = "bearingpointgmbh_replicon_admin"
http_conn_id = f'bearingpointgmbh_oauth2_http_conn_{instance}'

master_dag = f"bearingpoint_token_generation_master_{instance}"

client_id_secret_variable_name = f"bearingpoint_client_id_secret_variable_{instance}" #bearingpoint_client_id_secret_variable_prod
token_var = f"bearingpoint_token_variable_{instance}" #bearingpoint_token_variable_prod
