# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_import_project_fields.config import *

environment = 'production'

company_key = 'dxctechnology'
instance = "production_clone1"

replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = "/Inbound/Tasks/Processing1"
archive_filepath = "/Inbound/Tasks/Archives"
log_filepath = "/Inbound/Tasks/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task'
can_run_batch_task_var_name_child_dag = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task_child_dag'

child_dag_sync_gsap_task_max_active_runs = 20
child_dag_sync_gsap_task_system_level = 20
child_dag_sync_each_attribute_project_level_max_active_runs = 20
child_wbs_dag_sync_gsap_task_max_active_runs = 20
disabled = True
