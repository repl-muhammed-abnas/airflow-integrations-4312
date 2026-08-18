# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_les_uk_ireland_termination_balance_v2.config import *
from dxctechnology.lcsc_les_uk_ireland_termination_balance_v2.mapper.locations_company_codes_mapper import LOCATIONS_COMPANY_CODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_termination_balance_v2.mapper.termination_balance_req_mapper import TERMINATION_BALANCE_REQ_DATA
from dxctechnology.lcsc_les_uk_ireland_termination_balance_v2.mapper.payroll_calendar_mapper import LCSC_PAYROLL_CALENDAR, LES_PAYROLL_CALENDAR

instance = 'sandbox'

company_key = 'dxcsandbox'
environment = 'pre-production'

replicon_conn_id = 'dxcsandbox_replicon_RepliconIntWDPayroll'
sftp_conn_id = 'dxcsandbox_ADP_LCSC_LES_US_export_SFTP'
pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'

output_filepath = "/put/"
log_filepath = "/put/"
secondary_sftp_conn_id = 'dxctechnology_payroll_secondary_sftp'
secondary_output_filepath = '/DXC/UKIE_CSC_ES_TerminationBalanceExport/sandbox_output/'

tertiary_encrypted_filepath = '/Test/Outbound/PayrollTime/'
tertiary_log_filepath = '/Test/Outbound/PayrollTime/'
tertiary_sftp_conn_id = 'sftp-dxcsandbox_auspayroll-628172'
tertiary_pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'
can_upload_to_tertiary_sftp = True

# Monthly payroll calendar-driven schedule
# Runs daily at 00:00 GMT, validates against payroll calendar
time_zone = "Europe/London"
schedule_interval = '0 0 * * *'
lcsc_payroll_calendar = LCSC_PAYROLL_CALENDAR
les_payroll_calendar = LES_PAYROLL_CALENDAR

file_name_prefix = 'PQ0220'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

locations_company_codes_mapper = LOCATIONS_COMPANY_CODES_MAPPER
termination_balance_req_data = TERMINATION_BALANCE_REQ_DATA

version = "v2"

can_run_batch_task_var_name = f"dxctechnology_lcsc_les_uk_ireland_termination_balance_can_run_batch_task_{instance}"
master_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_master_{instance}_{version}'
process_termination_balance_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_process_location_company_code_wise_child_{instance}_{version}'
process_udf_update_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_process_udf_update_child_{instance}_{version}'
