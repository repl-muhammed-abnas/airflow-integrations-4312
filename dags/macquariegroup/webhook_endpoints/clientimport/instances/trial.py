# pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.webhook_endpoints.clientimport.config import *

instance = "trial"
environment = "pre-production"

company_key = "MacquarieProductiontrial"
replicon_conn_id = "macquarieproduction_replicon_ltran17trial"
sftp_conn_id = 'macquarieproduction_sftp_22007trial'


master_ondemand_trigger_dag_id = f"macquarie_ondemand_initiate_clientimport_{instance}"
hmac_secret_var = 'airflow_connector_ui_hmac_secret'