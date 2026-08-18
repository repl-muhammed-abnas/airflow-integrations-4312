# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.australia_termination_balance_v2.config import *

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

aus_user_report_name = "AUS user details - termination balance"
termination_balance_report_name_us = "Termination balance AUS report"

encrypt_output_file = True

division_name_es = ['AUES']
division_name_gsap = ['3001', '3124', '1602', '3118']
