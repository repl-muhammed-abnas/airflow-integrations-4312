# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.enterprise_project_import_v1.config import *

environment = 'pre-production'

instance = "sandbox"

company_key = "alvarezandmarsalsb"
bearer_token_var = 'alvarezandmarsalsb_enterprise_project_import_token'

replicon_conn_id = "alvarezandmarsalsb_replicon_repliconint.projectimport"
sftp_conn_id = "sftp_alvarezandmarsalsb_621229"

log_filepath = "/SB/Enterprise (Internal) Projects/Logs"

tenant_email = 'ITERP@alvarezandmarsal.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


master_dag = f"alvarezandmarsalholdings_enterprise_project_import_master_v1_{instance}"
process_projects = f"alvarezandmarsalholdings_enterprise_project_import_process_projects_child_v1_{instance}"
process_add_resource = f"alvarezandmarsalholdings_enterprise_project_import_process_add_resource_child_v1_{instance}"
process_remove_resource = f"alvarezandmarsalholdings_enterprise_project_import_process_remove_resource_child_v1_{instance}"
process_tasks = f"alvarezandmarsalholdings_enterprise_project_import_process_tasks_child_v1_{instance}"
process_log_generation = f"alvarezandmarsalholdings_enterprise_project_import_process_log_generation_child_v1_{instance}"
can_run_batch_task = f'alvarezandmarsalholdings_enterprise_project_import_batch_task_var_v1_{instance}'

disabled=True
