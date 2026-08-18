# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_task_import.config import *

region = 'us-east-2'
environment = 'pre-production'

company_key = 'DXCSandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntC1'
instance = "DXCSandbox2"

sftp_conn_id = "dxcsandbox2-sftp-628172_C1"

input_filepath = "/Test/Inbound/C1TaskandLabortypes/TaskInput"
archive_filepath = "/Test/Inbound/C1TaskandLabortypes/Archive"
log_filepath = "/Test/Inbound/C1TaskandLabortypes/TaskInputLogs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

division_variable = f"dxctechnology_c1_task_child_wbs_valid_divisions_{instance}"
