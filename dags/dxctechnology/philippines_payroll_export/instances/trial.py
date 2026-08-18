# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.philippines_payroll_export.config import *
from dxctechnology.philippines_payroll_export.mapper.paycodes import Regular_Paycodes
from dxctechnology.philippines_payroll_export.mapper.paycodes import Timeoff_Paycodes
from dxctechnology.philippines_payroll_export.mapper.payroll_calendar_mapper import PAYROLL_CALENDAR

regular_paycodes_mapper = Regular_Paycodes
timeoff_paycodes = Timeoff_Paycodes
PAYROLL_CALENDAR = PAYROLL_CALENDAR

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01'
pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'
sftp_conn_id = 'sftp_internal_useast2'
secondary_sftp_conn_id = 'sftp_internal_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

output_filepath = '/DXC/payrollexport/philippines/'
log_filepath = '/DXC/payrollexport/philippines/logs/'
secondary_filepath = '/DXC/payrollexport/philippines/backup/'

master_dag_id = f'dxctechnology_philippines_payroll_export_master_{instance}'
regular_child_dag_id = f'dxctechnology_process_philippines_export_regular_child_{instance}'
timeoff_child_dag_id = f'dxctechnology_process_philippines_export_timeoff_child_{instance}'
can_run_batch_task_var_name = f'dxctechnology_process_philippines_export_can_run_batch_task_{instance}'
can_encrypt_file = f"dxctechnology_process_philippines_export_can_encrypt_file_{instance}"
