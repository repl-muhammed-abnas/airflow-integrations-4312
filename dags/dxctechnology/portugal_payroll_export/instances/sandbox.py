# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.portugal_payroll_export.config import *
from dxctechnology.portugal_payroll_export.mapper.paycodes import Regular_Paycodes
from dxctechnology.portugal_payroll_export.mapper.paycodes import Timeoff_Paycodes

regular_paycodes_mapper = Regular_Paycodes
timeoff_paycodes = Timeoff_Paycodes

instance = 'sandbox'
environment = 'pre-production'

company_key = 'dxcsandbox'

replicon_conn_id = 'dxcsandbox_replicon_RepliconIntWDPayroll'
pgp_conn_id = 'pgp_dxctechnology_portugal_ADP'
sftp_conn_id = 'sftp_dxcsandbox_payrollexport'
secondary_sftp_conn_id = 'sftp_dxcsandbox_useast_prod'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

output_filepath = '/in'
log_filepath = '/in'
secondary_filepath = '/DXC/outbound/test/payrollexport/Portugal/'

child_dag_id = f'dxctechnology_process_portugal_export_child_{instance}'
can_run_batch_task_var_name = f'dxctechnology_process_portugal_export_can_run_batch_task_{instance}'
