#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.australia_payroll_extract.config import *
from dxctechnology.australia_payroll_extract.mapper.absence_taken_mapper import TimeOffMapper
from dxctechnology.australia_payroll_extract.mapper.absence_taken_mapper import ABSENCE_TAKEN_MAPPER

time_off_mapper = TimeOffMapper
absence_taken_mapper = ABSENCE_TAKEN_MAPPER

instance = 'DXCSandbox'
company_key = 'DXCSandbox'
environment = 'pre-production'

#Using the same connections used for USLES payroll export
replicon_conn_id = 'dxcsandbox_replicon_RepliconIntWDPayroll'
sftp_conn_id = 'dxcsandbox_ADP_LCSC_LES_US_export_SFTP'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc'
secondary_sftp_conn_id = 'sftp-dxcsandbox_auspayroll-628172'
secondary_encrypted_sftp_conn_id = 'sftp-dxcsandbox_auspayroll-628172_AUSPayroll'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

file_name_prefix="PQ0220"
encyrpt_file = True
active_user_report_name = 'Active User balance AUS report'
sell_back_report_name= 'Sell Back balance AUS report'
user_schedule_report_name= 'Schedule Report for AUS'

export = "Yes"
gsap_region = 'GSAP'
es_region = 'ES'

max_active_runs = 1
execution_timeout_days = 14
child_dag_max_active_runs = 10
duration_days = 84

output_filepath = "/put/"
log_filepath = "/put/"
unencrypted_filepath ="/Test/Outbound/PayrollTime/AUSTRALIA/unencrypted_files/"
secondary_output_filepath = '/Test/Outbound/PayrollTime/AUSTRALIA/sandbox_output/'
reference_file_path = '/Test/Outbound/PayrollTime/AUSTRALIA/reference/'
reference_file_archive_path = '/Test/Outbound/PayrollTime/AUSTRALIA/reference_archive/'
secondary_encrypted_output_filepath = '/Payroll Export/'

utc_timezone = "UTC"
ist_timezone = "Asia/Kolkata"
schedule_interval_es = '0 7 3,23 * *'
schedule_interval_gsap = '0 13 * * FRI'
schedule_interval_active_users = '0 13 28-31 * *'
schedule_interval_user_schedule = '0 14 * * *'
can_run_batch_task_var_name = ''

division_name_es = ['AUES']
division_name_gsap = ['3001', '3124', '1602', '3118']
