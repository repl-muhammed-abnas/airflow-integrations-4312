from azenta.oracle_project_sync.config import *

instance = 'qa'
environment = 'qa'
company_key = 'AzentaUSInctrial01'

replicon_conn_id = 'Azentausinctrial01_replicon_qa'

oracle_conn_id = 'Azentausinctrial01_oracle_fusion_qa'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f'Azenta_oracle_project_sync_master_{instance}'
process_project_dag_id = f'Azenta_oracle_project_sync_process_project_{instance}'
process_project_tasks_dag_id = f'Azenta_oracle_project_sync_process_project_tasks_{instance}'
process_log_generation = f'Azenta_oracle_project_sync_log_generation_{instance}'

watermark_var_name = f'azenta_oracle_project_sync_lastsync_{instance}'

can_run_batch_task_var_name = f'azenta_oracle_project_sync_{instance}_can_run_batch_task'

# ---------------------------------------------------------------------------
# Oracle Fusion REST API plumbing
# ---------------------------------------------------------------------------

ORACLE_API_VERSION = '11.13.18.05'
ORACLE_API_BASE = f'emmw-test.fa.us2.oraclecloud.com/fscmRestApi/resources/{ORACLE_API_VERSION}'
