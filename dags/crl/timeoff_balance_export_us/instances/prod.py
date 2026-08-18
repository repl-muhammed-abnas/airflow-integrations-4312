# pylint: disable=wildcard-import unused-wildcard-import
from crl.timeoff_balance_export_us.config import *
from crl.timeoff_balance_export_us.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER

instance = 'prod'

region = 'us-east-1'
environment = 'production'

company_key = 'CharlesRiverLaboratories'
replicon_conn_id = 'CharlesRiverLaboratories_replicon_Repliconint_payrollexport'
sftp_conn_id = 'sftp_charlesriverlaboratories_gvecrep476'
pgp_conn_id = 'pgp_crl_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratories_603355'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'



output_filepath = '/put'
secondary_encrypted_output_filepath = '/Production/Outbound/USA ADP Payroll'
secondary_output_filepath = '/Production/Outbound/USA ADP Payroll'

time_zone = "America/New_York"


CANADA_PAYROLL_CALENDER_MAPPER_TO_USE = CANADA_PAYROLL_CALENDER_MAPPER

adp_gv_system = 'P'
gv_system_number = '1'

crl_timeoff_balance_export_master = f"crl_timeoff_balance_export_master_usa_{instance}"
crl_daily_timeoff_balance_export_master = f"crl_daily_timeoff_balance_export_master_usa_{instance}"
crl_dec_31st_udf_update_master = f"crl_dec_31st_udf_update_master_usa_{instance}"
process_udf_update_child_dag = f"crl_timeoff_process_udf_update_child_usa_{instance}"
# pylint: disable=line-too-long
expected_report_columns = "Employee ID,Login Name,useruri,Time Off Type,Time Off Balance,Sick Payout Eligible,User Start Date,User End Date"
