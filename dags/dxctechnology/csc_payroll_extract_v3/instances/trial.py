#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.csc_payroll_extract_v3.config import *

instance = 'trial'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'dxctechnology-ftp'
pgp_conn_id = 'pgp_dxctechnology_ppmc_import'
max_active_runs = 10
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
time="23:49:00"
file_name_prefix="PQ3220"
eastern_timezone= "US/Eastern"
export = "Yes"
frequency = "Friday"
execution_timeout_days = 14
child_dag_max_active_runs = 10
output_filepath = "/DXC/USCSC_Payrollexport/"
log_filepath = "/DXC/USCSC_Payrollexport/logs/"
unencrypted_filepath ="/DXC/USCSC_Payrollexport/unencrypted_files/"
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
schedule_interval = '59 23 * * 5'
duration_days = 84
file_format='CSC_ADP_Export'
company_codes = '1102,1219,1103,1105'

disable=True

disabled=True
