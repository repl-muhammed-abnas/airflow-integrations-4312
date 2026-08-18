# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.philippines_payroll_export.config import *
from dxctechnology.philippines_payroll_export.mapper.paycodes import Regular_Paycodes
from dxctechnology.philippines_payroll_export.mapper.paycodes import Timeoff_Paycodes
from dxctechnology.philippines_payroll_export.mapper.payroll_calendar_mapper import PAYROLL_CALENDAR

regular_paycodes_mapper = Regular_Paycodes
timeoff_paycodes = Timeoff_Paycodes
PAYROLL_CALENDAR = PAYROLL_CALENDAR

instance = 'prod'
environment = 'production'

company_key = 'dxctechnology'

replicon_conn_id = 'dxctechnology_replicon_RepliconIntWDPayroll'
pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'
sftp_conn_id = 'sftp-dxctechnology_auspayroll-628172'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

output_filepath = '/Production/Outbound/PayrollTime/Philippines/'
log_filepath = '/Production/Outbound/PayrollTime/Philippines/logs/'

master_dag_id = f'dxctechnology_philippines_payroll_export_master_{instance}'
regular_child_dag_id = f'dxctechnology_process_philippines_export_regular_child_{instance}'
timeoff_child_dag_id = f'dxctechnology_process_philippines_export_timeoff_child_{instance}'
can_run_batch_task_var_name = f'dxctechnology_process_philippines_export_can_run_batch_task_{instance}'
can_encrypt_file = f"dxctechnology_process_philippines_export_can_encrypt_file_{instance}"
