# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_les_uk_ireland_termination_balance.config import *
from dxctechnology.lcsc_les_uk_ireland_termination_balance.mapper.locations_company_codes_mapper import LOCATIONS_COMPANY_CODES_MAPPER
from dxctechnology.lcsc_les_uk_ireland_termination_balance.mapper.termination_balance_req_mapper import TERMINATION_BALANCE_REQ_DATA

instance = 'trial'

company_key = 'dxctrial01'
environment = 'pre-production'

replicon_conn_id = 'dxctrial01_replicon_RepliconIntC1'
sftp_conn_id = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_dxctechnology_philippines_ADP'

output_filepath = "/DXC/UKIE_CSC_ES_TerminationBalanceExport/"
log_filepath = "/DXC/UKIE_CSC_ES_TerminationBalanceExport/logs/"
secondary_sftp_conn_id = 'dxctechnology_payroll_secondary_sftp'
secondary_output_filepath = '/DXC/UKIE_CSC_ES_TerminationBalanceExport/sandbox_output/'

tertiary_encrypted_filepath = ''
tertiary_log_filepath = ''
tertiary_sftp_conn_id = ''
tertiary_pgp_conn_id = ''
can_upload_to_tertiary_sftp = False

time_zone = "Europe/Paris"
schedule_interval = '30 0 * * 1'
duration_days = 84

file_name_prefix = 'PQ0220'

tenant_email = '{{ var.value.dagrun_internal_testing_email  }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

locations_company_codes_mapper = LOCATIONS_COMPANY_CODES_MAPPER
termination_balance_req_data = TERMINATION_BALANCE_REQ_DATA

can_run_batch_task_var_name = f"dxctechnology_lcsc_les_uk_ireland_termination_balance_can_run_batch_task_{instance}"
master_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_master_{instance}'
process_termination_balance_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_process_location_company_code_wise_child_{instance}'
process_udf_update_child_dag_id = f'dxctechnology_lcsc_les_uk_ireland_termination_balance_process_udf_update_child_{instance}'
# disabled=True
