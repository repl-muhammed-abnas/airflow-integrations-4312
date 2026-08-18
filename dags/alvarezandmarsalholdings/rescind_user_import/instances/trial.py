# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.rescind_user_import.config import *

environment = 'pre-production'

instance = "trial"

company_key = "AlvarezandMarsalHoldingsDevtrial01"

replicon_conn_id = "alvarezandmarsalholdingsdevtrial01_replicon_radmin1"
sftp_conn_id = "sftp_useast2"

log_filepath = "/Dev/Rescind/Logs"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"alvarezandmarsalholdings_rescind_user_import_master_{instance}"
process_disable_users_dag_id = f"alvarezandmarsalholdings_rescind_user_import_disable_user_child{instance}"
process_log_generation_dag_id = f"alvarezandmarsalholdings_rescind_user_import_process_logs_child{instance}"
can_run_batch_task = f'alvarezandmarsalholdings_rescind_user_import_batch_task_var_{instance}'

disabled=True
