# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.user_import_v1.config import *


instance = "trial"
environment = 'pre-production'
company_key = 'eisneramperafmig'
replicon_conn_id = 'eisneramperafmig_replicon_radmin'

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

bearer_token_var = f'eisneramper_user_import_secret{instance}'

master_dag = f'eisner_amper_user_import_master_{instance}_v1'
user_sync_child_dag_id = f"eisner_amper_user_import_sync_child_{instance}_v1"
process_each_user_dag_id = f"eisner_amper_user_import_process_each_user_{instance}_v1"
disble_user_dag_id = f"eisner_amper_disable_user_child_{instance}_v1"
update_user_dag_id = f"eisner_amper_update_user_child_{instance}_v1"
user_sync_cost_center_child_dag_id = f"eisner_amper_coster_center_child_{instance}_v1"
user_sync_company_code_child_dag_id = f"eisner_amper_company_code_child_{instance}_v1"
user_sync_work_location_child_dag_id = f"eisner_amper_work_location_child_{instance}_v1"
user_sync_roles_child_dag_id = f"eisner_amper_roles_child_{instance}_v1"
add_user_dag_id = f"eisner_amper_add_user_child_{instance}_v1"
