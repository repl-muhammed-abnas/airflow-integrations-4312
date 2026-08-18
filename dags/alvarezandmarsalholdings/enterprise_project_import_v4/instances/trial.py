# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.enterprise_project_import_v4.config import *

environment = 'pre-production'

instance = "trial"

company_key = "alvarezandmarsalholdingsdev"
bearer_token_var = 'alvarezandmarsalholdingsdev_enterprise_project_import_token'

replicon_conn_id = "alvarezandmarsalholdingsdev_replicon_radmin.1"
sftp_conn_id = "sftp_useast2"

log_filepath = "/Dev/Enterprise (Internal) Projects/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = 'v4'
dag_post_fix = f'{version}_{instance}'

master_dag = f"alvarezandmarsalholdings_enterprise_project_import_master_{dag_post_fix}"
process_projects = f"alvarezandmarsalholdings_enterprise_project_import_process_projects_child_{dag_post_fix}"
process_add_resource = f"alvarezandmarsalholdings_enterprise_project_import_process_add_resource_child_{dag_post_fix}"
process_remove_resource = f"alvarezandmarsalholdings_enterprise_project_import_process_remove_resource_child_{dag_post_fix}"
process_tasks = f"alvarezandmarsalholdings_enterprise_project_import_process_tasks_child_{dag_post_fix}"
process_log_generation = f"alvarezandmarsalholdings_enterprise_project_import_process_log_generation_child_{dag_post_fix}"
can_run_batch_task = f'alvarezandmarsalholdings_enterprise_project_import_batch_task_var_{dag_post_fix}'
