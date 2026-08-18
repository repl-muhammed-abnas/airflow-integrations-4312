#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.csc_payroll_extract.config import *

instance = 'trial'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'dxctechnology-ftp'
pgp_conn_id = 'pgp_dxctechnology_ppmc_import'
max_active_runs = 10
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
time_17_15 = "17:15:00"
time_19_15="19:15:00"
eastern_timezone= "US/Eastern"
export = "Yes"
frequency = "Friday"
execution_timeout_days = 14
child_dag_max_active_runs = 12
output_filepath = "/DXC/USCSC_Payrollexport/"
log_filepath = "/DXC/USCSC_Payrollexport/logs/"
unencrypted_filepath ="/DXC/USCSC_Payrollexport/unencrypted_files/"
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
# 15 minutes, 19 hours in EST on friday
schedule_interval_19_15 = '15 19 * * 5'
schedule_interval_17_15 = '15 17 * * 5'
duration_days = 84
