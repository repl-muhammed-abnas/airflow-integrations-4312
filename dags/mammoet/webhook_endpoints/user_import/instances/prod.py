# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.user_import_v1.config import *

instance = "prod"

environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"
sftp_conn_id = "sftp_mammoet_uat"
log_filepath = "/User Import/Production/Log"

mammoet_user_import_bearer_token_variable = f"mammoet_user_import_bearer_token_variable_{instance}"

tenant_email = 'repliconnotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

user_import_master_dag_id = f"mammoet_user_import_master_webhook_{instance}"
user_import_process_payload_child_dag_id = f"mammoet_user_import_process_payload_child_{instance}_v4"
