# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_import_project_fields.config import *

environment = 'pre-production'

company_key = 'dxcsandbox'
instance = "sandbox"

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = "/Inbound/Tasks/Processing"
archive_filepath = "/Inbound/Tasks/Archives"
log_filepath = "/Inbound/Tasks/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task'
can_run_batch_task_var_name_child_dag = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task_child_dag'
disabled = True
