#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_les_uk_ireland_payroll_extract.config import *
from dxctechnology.lcsc_les_uk_ireland_payroll_extract.mapper.locations_company_codes_mapper import LOCATIONS_COMPANY_CODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_payroll_extract.mapper.les_paycodes_mapper import LES_WAGECODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_payroll_extract.mapper.lcsc_paycodes_mapper import LCSC_WAGECODES_MAPPER

instance = 'production'

company_key = 'DXCTechnology'
environment = 'production'

replicon_conn_id = 'dxctechnology_replicon_RepliconIntWDPayroll'
sftp_conn_id = 'dxctechnology_ADP_LCSC_LES_US_export_SFTP'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'

frequency = "Monday"
time_zone = "Europe/Paris"
schedule_interval = '30 0 * * 1'
duration_days = 84

output_filepath = "/put/"
log_filepath = "/put/"
unencrypted_filepath ="/DXC/UKIE_CSC_ES_Payrollexport/unencrypted_files/"
secondary_sftp_conn_id = 'dxctechnology_integrations_cshare_secondary_sftp'
secondary_output_filepath ='/DXC/UKIE_CSC_ES_Payrollexport/'

tertiary_encrypted_filepath = '/Production/Outbound/PayrollTime/'
tertiary_log_filepath = '/Production/Outbound/PayrollTime/'
tertiary_sftp_conn_id = 'sftp-dxctechnology_auspayroll-628172'
tertiary_pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'
can_upload_to_tertiary_sftp = True

max_active_runs = 1
execution_timeout_days = 14
child_dag_max_active_runs = 10

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

file_name_prefix="PP3220"
export = "Yes"

locations_company_codes_mapper = LOCATIONS_COMPANY_CODES_MAPPER
lcsc_wage_codes_mapper = LCSC_WAGECODES_MAPPER
les_wage_codes_mapper = LES_WAGECODES_MAPPER
master_dag_id = f'dxctechnology_lcsc_les_uk_ireland_payroll_extract_master_{instance}'
process_payroll_data_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_payroll_extract_process_payroll_on_location_company_code_wise_child_{instance}'
