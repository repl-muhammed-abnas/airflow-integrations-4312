# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.disable_user.config import *

instance = "uat"

environment = "pre-production"

company_key = "mammoettrial01"

replicon_conn_id = "mammoettrial01_replicon_admin"
sftp_conn_id = "sftp_mammoet_uat"
log_filepath = "/User Import/Trial01/Log"

tenant_email = 'repliconnotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

disable_user_child_dagid = f"mammoet_user_import_disable_user_child_{instance}_v2"
disable_user_main_dagid = f"mammoet_user_import_disable_user_master_{instance}_v2"


can_run_batch_task_var_name = f"mammoet_user_import_can_run_batch_task_var_{instance}"
