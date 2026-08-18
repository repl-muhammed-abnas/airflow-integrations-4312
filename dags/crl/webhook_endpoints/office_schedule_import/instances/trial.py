from crl.webhook_endpoints.office_schedule_import.config import *
from crl.office_schedule_import_v1.instances.trial import master_dag_id

instance = "trial"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriestrial01"
replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_repliconadmin"


webhook_master_dagid = f"crl_office_schedule_import_webhook_master_{instance}"
office_schedule_import_master_dag_id = master_dag_id

bearer_token_var = f"crl_office_schedule_import_webhook_bearer_token_variable_{instance}"
