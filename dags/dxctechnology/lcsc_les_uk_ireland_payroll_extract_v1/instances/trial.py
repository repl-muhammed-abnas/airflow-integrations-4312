#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.config import *
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.mapper.locations_company_codes_mapper import LOCATIONS_COMPANY_CODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.mapper.les_paycodes_mapper import LES_WAGECODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.mapper.lcsc_paycodes_mapper import LCSC_WAGECODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.mapper.payroll_calendar_mapper import LCSC_PAYROLL_CALENDAR, LES_PAYROLL_CALENDAR

instance = 'trial'

company_key = 'dxctrial01'
environment = 'pre-production'

replicon_conn_id = 'dxctrial01_replicon_RepliconIntC1'
sftp_conn_id = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'

# Monthly payroll calendar-driven schedule
# Runs daily at 00:00 GMT, validates against payroll calendar
time_zone = "Europe/London"
schedule_interval = '0 0 * * *'
lcsc_payroll_calendar = LCSC_PAYROLL_CALENDAR
les_payroll_calendar = LES_PAYROLL_CALENDAR

output_filepath = "/DXC/UKIE_CSC_ES_Payrollexport/"
log_filepath = "/DXC/UKIE_CSC_ES_Payrollexport/logs/"
secondary_sftp_conn_id = 'dxctechnology-ftp'
secondary_output_filepath ='/DXC/UKIE_CSC_ES_Payrollexport/'

tertiary_encrypted_filepath = ''
tertiary_log_filepath = ''
tertiary_sftp_conn_id = ''
tertiary_pgp_conn_id = ''
can_upload_to_tertiary_sftp = False

max_active_runs = 1
execution_timeout_days = 14
child_dag_max_active_runs = 10

tenant_email = '{{ var.value.dagrun_internal_testing_email  }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

file_name_prefix="PQ0220"
export = "Yes"

locations_company_codes_mapper = LOCATIONS_COMPANY_CODES_MAPPER
lcsc_wage_codes_mapper = LCSC_WAGECODES_MAPPER
les_wage_codes_mapper = LES_WAGECODES_MAPPER

version = "v1"

master_dag_id = f'dxctechnology_lcsc_les_uk_ireland_payroll_extract_master_{instance}_{version}'
process_payroll_data_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_payroll_extract_process_payroll_on_location_company_code_wise_child_{instance}_{version}'
#disabled=True
