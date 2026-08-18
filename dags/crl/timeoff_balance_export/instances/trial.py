# pylint: disable=wildcard-import unused-wildcard-import
from crl.timeoff_balance_export.config import *
from crl.timeoff_balance_export.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'

company_key = 'CharlesRiverLaboratoriestrial01'
replicon_conn_id = 'charlesriverlaboratoriestrial01_replicon_riteam'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriestrial01_adp'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

encyrpt_file = False
max_active_runs = 1
execution_timeout_days = 14
child_dag_max_active_runs = 10
duration_days = 84

output_filepath = '/interfaces/PQ3/GVE220/put'
secondary_sftp_conn_id = 'sftp_useast2'
secondary_encrypted_output_filepath = '/CRLTrial/interfaces/PQ3/GVE220/put'

time_zone = "US/Eastern"


jan_1st_schedule_interval = "0 18 1 1 *"
dec_31st_schedule_interval = "0 18 31 12 *"
daily_schedule_interval = "0 18 * * *"
CANADA_PAYROLL_CALENDER_MAPPER_TO_USE = CANADA_PAYROLL_CALENDER_MAPPER

adp_gv_system = 'Q'
gv_system_number = '1'

crl_timeoff_balance_export_master = f"crl_timeoff_balance_export_master_{instance}"
crl_daily_timeoff_balance_export_master = f"crl_daily_timeoff_balance_export_master_{instance}"
crl_dec_31st_udf_update_master = f"crl_dec_31st_udf_update_master_{instance}"
process_udf_update_child_dag = f"crl_timeoff_process_udf_update_child_{instance}"
# pylint: disable=line-too-long
expected_report_columns = "Employee ID,Login Name,useruri,Time Off Type,Time Off Balance,Sick Payout Eligible,Banked Overtime Payout Eligible,User Start Date,User End Date"
