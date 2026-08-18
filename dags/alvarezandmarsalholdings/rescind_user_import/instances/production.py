from alvarezandmarsalholdings.rescind_user_import.config import *

environment = 'production'

instance = "prod"

company_key = "alvarezandmarsal"

replicon_conn_id = "alvarezandmarsal_replicon_repliconint.userimport"
sftp_conn_id = "sftp_alvarezandmarsal_621229"

log_filepath = "/Production/Rescind/Logs"

tenant_email = 'ITERP@alvarezandmarsal.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"alvarezandmarsalholdings_rescind_user_import_master_{instance}"
process_disable_users_dag_id = f"alvarezandmarsalholdings_rescind_user_import_disable_user_child{instance}"
process_log_generation_dag_id = f"alvarezandmarsalholdings_rescind_user_import_process_logs_child{instance}"
can_run_batch_task = f'alvarezandmarsalholdings_rescind_user_import_batch_task_var_{instance}'
