# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_us_termination_balance.config import *

instance = 'DXCSandbox2'
# 15 minutes, 19 hours in est on friday
schedule_interval = '15 19 * * 5'
eastern_timezone= "US/Eastern"
replicon_conn_id = 'dxcsandbox2_replicon_RepliconIntWDPayroll'
pgp_conn_id = 'pgp_dxctechnology_ppmc_import'
sftp_conn_id = 'dxcsandbox2_628172_LCSC_US_export'
output_filepath = "/Test/Outbound/PayrollTime/L-CSC US & CA/"
log_filepath = "/Test/Outbound/PayrollTime/L-CSC US & CA/Logs/"
company_key = 'DXCSandbox2'
file_name_prefix_US= 'USBalance.'
file_name_prefix_CA= 'CABalance.'
subtype_timeoff_name_US='[USA] Vacation Accrued'
subtype_timeoff_name_CA='[CAN] Vacation'
timeoff_type1_name_US='[USA] Vacation Accrued'
timeoff_type2_name_US='[USA] PTO Accrued'
timeoff_type1_name_CA='[CAN] Vacation'
timeoff_type2_name_CA='[CAN] Banked time'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
canada_user_report_name = "Canada user details - CSC  termination balance"
usa_user_report_name = "USA user details - CSC  termination balance"
termination_balance_report_name = "Termination balance CSC report"

encrypt_output_file_canada = False
encrypt_output_file_usa = False
