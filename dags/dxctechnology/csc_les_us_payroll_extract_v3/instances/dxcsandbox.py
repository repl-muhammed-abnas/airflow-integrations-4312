#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.csc_les_us_payroll_extract_v3.config import *

instance = 'DXCSandbox'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox_replicon_RepliconIntWDPayroll'
sftp_conn_id = 'dxcsandbox_ADP_LCSC_LES_US_export_SFTP'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
max_active_runs = 1
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
time_19_15="23:59:00"
eastern_timezone= "US/Eastern"
export = "Yes"
frequency = "Friday"
execution_timeout_days = 14
child_dag_max_active_runs = 10
file_name_prefix="PQ3220"
output_filepath = "/put/"
log_filepath = "/put/"
unencrypted_filepath ="/DXC/USCSC_Payrollexport/unencrypted_files/"
# 59 minutes, 23 hours in est on friday
schedule_interval_19_15 = '59 23 * * 5'
duration_days = 84
secondary_sftp_conn_id = 'dxctechnology_payroll_secondary_sftp'
secondary_output_filepath = '/dxc/lcscpayrollexport/sandbox_output/'

can_upload_to_tertiary_sftp = False

disable=True

disabled=True
