# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_labour_types_import_v1.config import *

instance = 'dxcsandbox2'
environment = 'pre-production'

company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntC1'
sftp_conn_id = "dxcsandbox2-sftp-628172_C1"

input_filepath = "/Test/Inbound/C1TaskandLabortypes/LaborTypeInput"
archive_filepath = "/Test/Inbound/C1TaskandLabortypes/Archive"
log_filepath = "/Test/Inbound/C1TaskandLabortypes/LaborTypeInputLogs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

division_variable = f"dxctechnology_c1_labour_types_child_wbs_valid_divisions_{instance}"

can_run_batch_task_var_name = f'dxctechnology_c1_labour_types_{instance}_can_run_batch_task'
