# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_les_us_termination_balance_v3.config import *

instance = 'dxctrial01'
# 59 minutes, 23 hours in est on friday
eastern_timezone= "US/Eastern"
schedule_interval = '59 23 * * 5'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'dxctechnology-ftp'
output_filepath = "/DXC/lscs_termination_balance/"
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
log_filepath = "/DXC/lscs_termination_balance/logs/"
company_key = 'dxctrial01'
file_name_prefix_US= 'PQ3220'
timeoff_type1_name_USCsc='[USA] Vacation Accrued'
timeoff_type2_name_USCsc='[USA] 21-PTO Accrued'
timeoff_type1_name_USLes='[USA] ES PTO Bank - 2651'
timeoff_type2_name_USLes='[USA] ES Excess FTO - 2615'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
usa_csc_user_report_name = "USA user details - CSC  termination balance"
usa_les_user_report_name = "USA user details - CSC  termination balance"
termination_balance_report_name_us = "Timeoff Termination Balance Report"
encrypt_output_file_canada = False
encrypt_output_file_usa = False
secondary_sftp_conn_id = 'dxctechnology_payroll_secondary_sftp'
secondary_output_filepath = '/dxc/lcscpayrollexport/sandbox_output/'

disable=True

disabled=True
