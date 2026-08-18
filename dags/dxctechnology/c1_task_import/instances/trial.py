# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_task_import.config import *

region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
instance = "dxctrial"

sftp_conn_id = "sftp_useast2"
input_filepath = "/Test/Inbound/C1TaskandLabortypes/TaskInput"
archive_filepath = "/Test/Inbound/C1TaskandLabortypes/Archive"
log_filepath = "/Test/Inbound/C1TaskandLabortypes/TaskInputLogs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

division_variable = f"dxctechnology_c1_task_child_wbs_valid_divisions_{instance}"

disable=True

disabled=True
