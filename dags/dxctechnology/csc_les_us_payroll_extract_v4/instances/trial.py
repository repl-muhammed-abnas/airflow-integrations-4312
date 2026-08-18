#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.csc_les_us_payroll_extract_v4.config import *

instance = 'trial'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'dxctechnology-ftp'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
max_active_runs = 1
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
time_19_15="23:59:00"
file_name_prefix="PQ3220"
export = "Yes"
frequency = "Friday"
execution_timeout_days = 14
child_dag_max_active_runs = 10
output_filepath = "/DXC/USCSC_Payrollexport/"
log_filepath = "/DXC/USCSC_Payrollexport/logs/"
unencrypted_filepath ="/DXC/USCSC_Payrollexport/unencrypted_files/"
# eastern time
eastern_timezone= "US/Eastern"
schedule_interval_19_15 = '59 23 * * 5'
duration_days = 84

disable=True

disabled=True
