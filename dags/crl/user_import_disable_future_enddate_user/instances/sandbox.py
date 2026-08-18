from crl.user_import_disable_future_enddate_user.config import *

instance = "sandbox"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"
replicon_conn_id = "charlesriverlaboratoriessandbox_repliconint_userimport"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable_user_master_dagid = f'crl_user_import_global_disable_future_enddate_user_master_{instance}'
disable_future_enddate_user_child_dagid = f'crl_user_import_global_disable_future_enddate_user_child_{instance}'

INTEGRATION_USERNAME = 'Admin, Replicon'
