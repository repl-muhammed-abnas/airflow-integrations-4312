from nber.project_import.config import *

instance = "uat"
environment = "pre-production"
company_key = "nbertrial01"

replicon_conn_id = "nbertrial01_replicon_repliconint"

master_dagid = f"nber_project_import_master_{instance}"
process_project_dagid = f"nber_project_import_process_projects_child_{instance}"

can_run_batch_task_var_name = "nber_project_import_can_run_batch_task"

tenant_email = "Repliconintegration@nber.org, krishnanistala@deltek.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_failure_alert_email }}"
