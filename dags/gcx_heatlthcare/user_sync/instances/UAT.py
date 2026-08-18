from gcx_heatlthcare.user_sync.config import *

instance = 'UAT'
company_key = 'gcxhealthcaretrial01'
replicon_conn_id = 'gcxhealthcaretrial01_replicon_adminr'
http_conn_id = 'gcxhealthcaretrial01_http'

token_var = 'gcxhealthcare_user_sync_token'

sftp_conn_id = 'sftp_useast2'

reference_filepath = "/gcx/reference/reference.csv"
archive_filepath = "/gcx/Archived/"

master_dag_name = f'gcx_healthcare_user_sync_master_{instance}'
create_manager_child_dag_id = f'gcx_healthcare_user_sync_create_manager_child_{instance}'
create_new_user_child_dag_id = f'gcx_healthcare_user_sync_create_new_user_child_{instance}'
update_user_child_dag_id = f'gcx_healthcare_user_sync_update_user_child_{instance}'

tenant_email = 'Michelle@Gcx.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_name = f'gcxhealthcare_user_sync_batch_task_{instance}'
