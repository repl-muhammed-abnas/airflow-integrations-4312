from crl.webhook_endpoints.office_schedule_import.config import *

from crl.office_schedule_import_v1.instances.prod import master_dag_id

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_replicon_Repliconint_userimport"


webhook_master_dagid = f"crl_office_schedule_import_webhook_master_{instance}"
office_schedule_import_master_dag_id = master_dag_id

bearer_token_var = f"crl_office_schedule_import_webhook_bearer_token_variable_{instance}"
