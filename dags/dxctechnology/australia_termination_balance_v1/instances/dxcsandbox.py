# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.australia_termination_balance_v1.config import *
from dxctechnology.australia_termination_balance_v1.mapper.absence_taken_mapper import TimeOffMapper
from dxctechnology.australia_termination_balance_v1.mapper.absence_taken_mapper import ABSENCE_TAKEN_MAPPER

time_off_mapper = TimeOffMapper
absence_taken_mapper = ABSENCE_TAKEN_MAPPER

instance = 'DXCSandbox'

utc_timezone= "Asia/Kolkata"
schedule_interval = '0 18 * * *'

company_key = 'DXCSandbox'

#Using the same connections used for USLES termination balance export
replicon_conn_id = 'dxcsandbox_replicon_RepliconIntWDPayroll'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
sftp_conn_id = 'dxcsandbox_ADP_LCSC_LES_US_export_SFTP'
secondary_sftp_conn_id = 'sftp-dxcsandbox-628172_payroll'
secondary_encrypted_sftp_conn_id = 'sftp-dxcsandbox_auspayroll-628172_AUSPayroll'

output_filepath = "/put/"
log_filepath = "/put/"
secondary_output_filepath = '/Test/Outbound/PayrollTime/AUSTRALIA/sandbox_output/'
secondary_encrypted_output_filepath = '/Payroll Export/'

file_name_prefix= 'PQ0220'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

termination_balance_report_name_us = "Termination balance AUS report"

encrypt_output_file = True
