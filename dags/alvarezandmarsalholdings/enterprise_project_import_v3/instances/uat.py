# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.enterprise_project_import_v3.config import *

environment = 'pre-production'

instance = "uat"

company_key = "alvarezandmarsalholdingsuat"
bearer_token_var = 'alvarezandmarsalholdingsuat_enterprise_project_import_token'

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin.1"
sftp_conn_id = "sftp_alvarezandmarsalholdingsuat_621229"

log_filepath = "/UAT/Enterprise (Internal) Projects/Logs"

tenant_email = 'ITERP@alvarezandmarsal.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = 'v3'
dag_post_fix = f'{version}_{instance}'

master_dag = f"alvarezandmarsalholdings_enterprise_project_import_master_{dag_post_fix}"
process_projects = f"alvarezandmarsalholdings_enterprise_project_import_process_projects_child_{dag_post_fix}"
process_add_resource = f"alvarezandmarsalholdings_enterprise_project_import_process_add_resource_child_{dag_post_fix}"
process_remove_resource = f"alvarezandmarsalholdings_enterprise_project_import_process_remove_resource_child_{dag_post_fix}"
process_tasks = f"alvarezandmarsalholdings_enterprise_project_import_process_tasks_child_{dag_post_fix}"
process_log_generation = f"alvarezandmarsalholdings_enterprise_project_import_process_log_generation_child_{dag_post_fix}"
can_run_batch_task = f'alvarezandmarsalholdings_enterprise_project_import_batch_task_var_{dag_post_fix}'
