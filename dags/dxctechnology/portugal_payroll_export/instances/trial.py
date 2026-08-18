# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.portugal_payroll_export.config import *
from dxctechnology.portugal_payroll_export.mapper.paycodes import Regular_Paycodes
from dxctechnology.portugal_payroll_export.mapper.paycodes import Timeoff_Paycodes

regular_paycodes_mapper = Regular_Paycodes
timeoff_paycodes = Timeoff_Paycodes

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01'
pgp_conn_id = 'pgp_dxctechnology_portugal_ADP'
sftp_conn_id = 'rsftp-useast_for_testing'
secondary_sftp_conn_id = 'rsftp-useast_for_testing'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

output_filepath = '/DXC/payrollexport/Portugal/'
log_filepath = '/DXC/payrollexport/Portugal/'
secondary_filepath = '/DXC/payrollexport/Portugal/'

child_dag_id = f'dxctechnology_process_portugal_export_child_{instance}'
can_run_batch_task_var_name = f'dxctechnology_process_portugal_export_can_run_batch_task_{instance}'

disable=True

disabled=True
