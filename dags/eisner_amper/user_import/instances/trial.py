# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.user_import.config import *

instance = '_trial'
company_key = 'eisnerampertrial02'
replicon_conn_id = 'eisnerampertrial02_replicon_radmin'
bearer_token_var = f'eisnerampertrial02_user_import_secret{instance}'

workato_api_endpoint = f'eisner_amper_user_import_webhook{instance}_workato_endpoint'

can_run_batch_task_var_name = f'eisner_amper_user_import_run_workato{instance}'

execution_timeout_days = 14
child_dag_max_active_runs = 2


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = "sftp_useast2"
sftp_conn_internal_id = "sftp_useast2"

client_user_log_path = '/Logging and Notifications/521759-EisnerAmper/User Sync Logs/'
internal_user_log_path = '/trial/User Import/Log/'

master_dag = f'eisner_amper_user_import_trigger_{instance}'
user_sync_child_dag_id = f"eisner_amper_user_import_sync_child_{instance}"
process_each_user_dag_id = f"eisner_amper_user_import_process_each_user_{instance}"
disble_user_dag_id = f"eisner_amper_disable_user_child_{instance}"
update_user_dag_id = f"eisner_amper_update_user_child_{instance}"
user_sync_cost_center_child_dag_id = f"eisner_amper_coster_center_child_{instance}"
user_sync_company_code_child_dag_id = f"eisner_amper_company_code_child_{instance}"
user_sync_work_location_child_dag_id = f"eisner_amper_work_location_child_{instance}"
user_sync_roles_child_dag_id = f"eisner_amper_roles_child_{instance}"
add_user_dag_id = f"eisner_amper_add_user_child_{instance}"

can_run_batch_task_name = f'eisner_amper_user_import_batch_run_{instance}'

disable = True

disabled = True
