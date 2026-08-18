from crl.user_import_disable_future_enddate_user.config import *

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_replicon_Repliconint_userimport"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable_user_master_dagid = f'crl_user_import_global_disable_future_enddate_user_master_{instance}'
disable_future_enddate_user_child_dagid = f'crl_user_import_global_disable_future_enddate_user_child_{instance}'

INTEGRATION_USERNAME = 'Admin, Replicon'