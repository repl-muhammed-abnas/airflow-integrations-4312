# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_les_uk_ireland_termination_balance.config import *
from dxctechnology.lcsc_les_uk_ireland_termination_balance.mapper.locations_company_codes_mapper import LOCATIONS_COMPANY_CODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_termination_balance.mapper.termination_balance_req_mapper import TERMINATION_BALANCE_REQ_DATA

instance = 'production'

company_key = 'DXCTechnology'
environment = 'production'

replicon_conn_id = 'dxctechnology_replicon_RepliconIntWDPayroll'
sftp_conn_id = 'dxctechnology_ADP_LCSC_LES_US_export_SFTP'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'

output_filepath = "/put/"
log_filepath = "/put/"
secondary_sftp_conn_id = 'dxctechnology_integrations_cshare_secondary_sftp'
secondary_output_filepath = '/DXC/UKIE_CSC_ES_TerminationBalanceExport/'

tertiary_encrypted_filepath = '/Production/Outbound/PayrollTime/'
tertiary_log_filepath = '/Production/Outbound/PayrollTime/'
tertiary_sftp_conn_id = 'sftp-dxctechnology_auspayroll-628172'
tertiary_pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'
can_upload_to_tertiary_sftp = True

time_zone = "Europe/Paris"
schedule_interval = '30 0 * * 1'
duration_days = 84

file_name_prefix = 'PP3220'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

locations_company_codes_mapper = LOCATIONS_COMPANY_CODES_MAPPER
termination_balance_req_data = TERMINATION_BALANCE_REQ_DATA

can_run_batch_task_var_name = f"dxctechnology_lcsc_les_uk_ireland_termination_balance_can_run_batch_task_{instance}"
master_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_master_{instance}'
process_termination_balance_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_process_location_company_code_wise_child_{instance}'
process_udf_update_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_process_udf_update_child_{instance}'
