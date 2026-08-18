# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.australia_termination_balance.config import *

instance = 'dxctrial01'

utc_timezone= "Asia/Kolkata"
schedule_interval = '0 18 * * *'

company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'rsftp-useast_for_testing'
secondary_sftp_conn_id = 'sftp_internal'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
secondary_encrypted_sftp_conn_id = 'rsftp-useast_for_testing'

output_filepath = "/DXC/payrollexport/AUS/"
secondary_output_filepath = '/DXC/payrollexport/AUS/unencrypted_files/'
log_filepath = "/DXC/payrollexport/AUS/Logs/"
secondary_encrypted_output_filepath = '/DXC/payrollexport/AUS/'

file_name_prefix= 'PQ3220'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

aus_user_report_name = "AUS user details - termination balance"
termination_balance_report_name_us = "Termination balance AUS report"

encrypt_output_file = False

division_name_es = ['AUES']
division_name_gsap = ['3001', '3124', '1602', '3118']

report_data = False
