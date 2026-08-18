# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.australia_termination_balance_v2.config import *

instance = 'production'
region = 'us-east-2'
utc_timezone= "Asia/Kolkata"
schedule_interval = '0 18 * * *'

company_key = 'DXCTechnology'
environment = 'production'

#Using the same connections used for USLES termination balance export
replicon_conn_id = 'DXCTechnology_http_RepliconIntWDPayroll'
sftp_conn_id = 'dxctechnology_ADP_LCSC_LES_US_export_SFTP'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
secondary_sftp_conn_id = 'sftp-dxctechnology_auspayroll-628172'
secondary_encrypted_sftp_conn_id = 'sftp-dxctechnology_auspayroll-628172_AUSPayroll'

output_filepath = "/put/"
log_filepath = "/put/"
secondary_output_filepath = '/Production/Outbound/Australia/production_output/'
secondary_encrypted_output_filepath = '/Payroll Export/'

file_name_prefix= 'PP3220'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

aus_user_report_name = "AUS user details - termination balance"
termination_balance_report_name_us = "Termination balance AUS report"

encrypt_output_file = True

division_name_es = ['AUES']
division_name_gsap = ['3001', '3124', '1602', '3118']
