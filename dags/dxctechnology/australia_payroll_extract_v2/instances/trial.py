# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.australia_payroll_extract_v2.config import *
from dxctechnology.australia_payroll_extract_v2.mapper.absence_taken_mapper import TimeOffMapper
from dxctechnology.australia_payroll_extract_v2.mapper.absence_taken_mapper import ABSENCE_TAKEN_MAPPER

time_off_mapper = TimeOffMapper
absence_taken_mapper = ABSENCE_TAKEN_MAPPER

instance = 'trial'

replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_dxctechnology_adp_les_lcsc_trial'
secondary_encrypted_sftp_conn_id = 'rsftp-useast_for_testing'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

file_name_prefix = "PQ0220"
encyrpt_file = False
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

output_filepath = "/DXC/payrollexport/AUS/"
log_filepath = "/DXC/payrollexport/AUS/Logs/"
unencrypted_filepath = "/DXC/payrollexport/AUS/unencrypted_files/"
reference_file_path = '/DXC/payrollexport/AUS/Reference/'
reference_file_archive_path = '/DXC/payrollexport/AUS/Archive/'
secondary_encrypted_output_filepath = '/Payroll Export/'

utc_timezone = "UTC"
ist_timezone = "Asia/Kolkata"
schedule_interval_es = '0 7 * * *'
schedule_interval_gsap = '0 13 * * FRI'
schedule_interval_active_users = '0 13 * * *'
can_run_batch_task_var_name = f'dxctechnology_aus_payroll_export_{instance}_can_run_batch_task'

division_name_es = ['AUES']
division_name_gsap = ['3001', '3124', '1602', '3118']
disabled = True
