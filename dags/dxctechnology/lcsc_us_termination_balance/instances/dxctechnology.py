# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.lcsc_us_termination_balance.config import *

environment = 'production'
instance = 'DXCTechnology'
# 15 minutes, 19 hours in est on friday
schedule_interval = '15 19 * * 5'
eastern_timezone= "US/Eastern"
pgp_conn_id = 'pgp_dxctechnology_lcsc_payroll_export'
replicon_conn_id = 'DXCTechnology_http_RepliconIntWDPayroll'
sftp_conn_id = 'dxctechnology_628172_Payroll_export'
output_filepath = "/Production/Outbound/L-CSC US & CA/"
log_filepath = "/Production/Outbound/L-CSC US & CA/Logs/"
company_key = 'DXCTechnology'
file_name_prefix_US= 'USBalance.'
file_name_prefix_CA= 'CABalance.'
timeoff_type1_name_US='[USA] Vacation Accrued'
timeoff_type2_name_US='[USA] 21-PTO Accrued'
timeoff_type1_name_CA='[CAN] Vacation'
timeoff_type2_name_CA='[CAN] Banked time'
subtype_timeoff_name_US='[USA] Vacation Accrued'
subtype_timeoff_name_CA='[CAN] Vacation'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
canada_user_report_name = "Canada user details - CSC  termination balance"
usa_user_report_name = "USA user details - CSC  termination balance"
termination_balance_report_name = "Termination balance CSC report"
encrypt_output_file_canada = True
encrypt_output_file_usa = False
