from transparentbpo.project_and_task_sync.config import *

# AWS Configuration
instance = 'trial'
environment = 'pre-production'

# Instance Identification
company_key = "TransparentBPOafmig"

version = ''  # _v1, _v2

replicon_conn_id = 'transparentbpoafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

log_filepath = '/transparentBPO/project_task_sync'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_id = f'transparentbpo_project_and_task_sync_master_{instance}{version}'
process_logs_pregeneration_dag_id = f'transparentbpo_project_and_task_sync_process_logs_pregeneration_{instance}{version}'
process_send_logs_dag_id = f'transparentbpo_project_and_task_sync_process_logs_{instance}{version}'

lookup_log_timestamp_var = f'transparentbpo_project_and_task_sync_log_lookup_timestamp_{instance}'
