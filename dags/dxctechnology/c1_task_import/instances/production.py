# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_task_import.config import *

environment = 'production'

company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntC1'
instance = "production"

sftp_conn_id = "sftp_dxctechnology_c1"

input_filepath = "/Production/Inbound/C1TaskandLabortypes/TaskInput"
log_filepath = "/Production/Inbound/C1TaskandLabortypes/TaskInputLogs"
archive_filepath = "/Production/Inbound/C1TaskandLabortypes/Archive"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

division_variable = f"dxctechnology_c1_task_child_wbs_valid_divisions_{instance}"
