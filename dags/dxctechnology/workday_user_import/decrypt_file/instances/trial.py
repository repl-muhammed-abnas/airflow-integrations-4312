# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import.decrypt_file.config import *

instance = "trial"

environment = "pre-production"
can_run_batch_task_var_name = f"dxctechnology_workday_user_import_can_run_batch_task_variable_{instance}"

company_key = "dxctrial01"
replicon_conn_id = "dxctrial01_replicon_x.replicon.workday1"
sftp_conn_id = "sftp_useast2"

pgp_conn_id = f"dxctechnology_workday_user_import_pgp_connection_{instance}"

input_file_path = "/WD/Input/local"
archive_file_path = "/WD/Archives/Decrypt"
log_file_path = "/WD/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_emails = "{{ var.value.dagrun_internal_testing_email }}"
