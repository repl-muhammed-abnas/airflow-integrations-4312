# pylint: disable=wildcard-import unused-wildcard-import
from dkpierceassociates.project_manager_sync.config import *

instance = "prod"

region = 'us-east-1'
environment = 'production'
company_key = 'dkpierceassociatesafmig'

replicon_conn_id = "replicon_dkpierceassociatesafmig_admin"
salesforce_conn_id = "standard_sf_dkpierceassociates_salesforce2"

master_dag_id = f'dkpierceassociates_create_replicon_project_manager_in_salesforce_master_{instance}'

