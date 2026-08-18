# WCG Project Sync v2 - Trial Instance Configuration
from wcg.project_sync.config import *

instance = "trial"
environment = "pre-production"

company_key = "WCGafmig"
replicon_conn_id = "wcgtrial01_replicon_admin"
sftp_conn_id = "sftp_useast2"

# Trial environment specific settings
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"

# Trial SFTP paths
input_filepath = "/Trial/WCG/ProjectSync/Input/"
input_archive_filepath = "/Trial/WCG/ProjectSync/Archive/"
logs_filepath = "/Trial/WCG/ProjectSync/Logs/"

# DAG IDs
master_dag_id = f"wcg_project_sync_master_{instance}"
process_project_child_dag_id = f"wcg_project_sync_process_project_child_{instance}"
update_subsidiary_dag_id = f"wcg_project_sync_update_subsidiary_{instance}"

# Batch task control
can_run_batch_task_var_name = f"wcg_project_sync_run_batch_task_{instance}"
