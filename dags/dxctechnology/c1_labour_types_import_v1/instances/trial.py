# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_labour_types_import_v1.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01'
sftp_conn_id = "sftp_useast2"

input_filepath = "/Test/Inbound/C1TaskandLabortypes/LaborTypeInput"
archive_filepath = "/Test/Inbound/C1TaskandLabortypes/Archive"
log_filepath = "/Test/Inbound/C1TaskandLabortypes/LaborTypeInputLogs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

division_variable = f"dxctechnology_c1_labour_types_child_wbs_valid_divisions_{instance}"

disable=True

disabled=True

can_run_batch_task_var_name = f'dxctechnology_c1_labour_types_{instance}_can_run_batch_task'
