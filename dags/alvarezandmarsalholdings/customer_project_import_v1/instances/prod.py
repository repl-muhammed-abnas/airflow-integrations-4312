# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.customer_project_import_v1.config import *

environment = 'production'

instance = "production"

company_key = "alvarezandmarsal"
bearer_token_var = 'alvarezandmarsal_project_import_customer_token'

replicon_conn_id = "alvarezandmarsal_replicon_repliconint.projectimport"
sftp_conn_id = "sftp_alvarezandmarsal_621229"

log_filepath = "/Production/Customer Projects/Logs"

tenant_email = 'ITERP@alvarezandmarsal.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


version = "v1" # v1, v2, v3 etc...
dag_sufix = f"{version}_{instance}"

master_dag = f"alvarezandmarsalholdings_project_import_customer_master_{dag_sufix}"
process_projects = f"alvarezandmarsalholdings_project_import_customer_process_projects_child_{dag_sufix}"
process_tasks = f"alvarezandmarsalholdings_project_import_customer_process_tasks_child_{dag_sufix}"
process_log_generation = f"alvarezandmarsalholdings_project_import_customer_process_log_generation_child_{dag_sufix}"
can_run_batch_task = f'alvarezandmarsalholdings_project_import_customer_batch_task_var_{dag_sufix}'
