# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_import.config import *

instance = 'dxctrial01'
company_key = 'dxctrial01'
replicon_conn_id = 'replicon-dxctechnology-ftp'
sftp_conn_id = 'dxctechnology-ftp'
input_filepath = '/Production/Inbound/CompassWBSImport'
archive_filepath = '/Production/Archive/CompassWBSImport'
log_filepath = '/Production/Logs/CompassWBSImport'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxc_compass_wbs_import_{instance}_can_run_batch_task'
