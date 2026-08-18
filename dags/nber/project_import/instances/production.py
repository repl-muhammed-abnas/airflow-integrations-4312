from nber.project_import.config import *

instance = "production"
environment = "production"
company_key = "nber"

replicon_conn_id = "nber_replicon_repliconint"

master_dagid = f"nber_project_import_master_{instance}"
process_project_dagid = f"nber_project_import_process_projects_child_{instance}"

can_run_batch_task_var_name = "nber_project_import_can_run_batch_task"

tenant_email = "Repliconintegration@nber.org"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alerts_email = "{{ var.value.dagrun_failure_alert_email }}"
