# pylint: disable=wildcard-import unused-wildcard-import
from crl.timeoff_balance_export_us.config import *
from crl.timeoff_balance_export_us.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'

company_key = 'CharlesRiverLaboratoriestrial01'
replicon_conn_id = 'charlesriverlaboratoriestrial01_replicon_riteam'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'



output_filepath = '/put'
secondary_encrypted_output_filepath = '/Test/Outbound/USA ADP Payroll'

time_zone = "America/New_York"


CANADA_PAYROLL_CALENDER_MAPPER_TO_USE = CANADA_PAYROLL_CALENDER_MAPPER

adp_gv_system = 'Q'
gv_system_number = '1'

crl_timeoff_balance_export_master = f"crl_timeoff_balance_export_master_usa_{instance}"
crl_daily_timeoff_balance_export_master = f"crl_daily_timeoff_balance_export_master_usa_{instance}"
crl_dec_31st_udf_update_master = f"crl_dec_31st_udf_update_master_usa_{instance}"
process_udf_update_child_dag = f"crl_timeoff_process_udf_update_child_usa_{instance}"
# pylint: disable=line-too-long
expected_report_columns = "Employee ID,Login Name,useruri,Time Off Type,Time Off Balance,Sick Payout Eligible,User Start Date,User End Date"

disabled=True
