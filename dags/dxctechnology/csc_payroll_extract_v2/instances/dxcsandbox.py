#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.csc_payroll_extract_v2.config import *

instance = 'DXCSandbox'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox_replicon_RepliconIntWDPayroll'
sftp_conn_id = 'dxcsandbox_628172_LCSC_US_export'
pgp_conn_id = 'pgp_dxcsandbox_lcsc_payroll_export'
max_active_runs = 10
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
time_17_15 = "17:15:00"
time_19_15="19:15:00"
eastern_timezone= "US/Eastern"
export = "Yes"
frequency = "Friday"
execution_timeout_days = 14
child_dag_max_active_runs = 12
output_filepath = "/Test/Outbound/PayrollTime/L-CSC US & CA/"
log_filepath = "/Test/Outbound/PayrollTime/L-CSC US & CA/Logs/"
unencrypted_filepath ="/DXC/USCSC_Payrollexport/unencrypted_files/"
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
# 15 minutes, 23 hours in utc on friday(eastern time converted to UTC)
schedule_interval_19_15 = '15 19 * * 5'
schedule_interval_17_15 = '15 17 * * 5'
duration_days = 84
secondary_sftp_conn_id = 'dxctechnology_payroll_secondary_sftp'
secondary_output_filepath = '/dxc/lcscpayrollexport/sandbox_output/'
