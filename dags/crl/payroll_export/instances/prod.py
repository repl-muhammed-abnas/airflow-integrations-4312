# pylint: disable=wildcard-import unused-wildcard-import
from crl.payroll_export.config import *
from crl.payroll_export.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER

instance = "prod"

environment = "production"

company_key = 'CharlesRiverLaboratories'
replicon_conn_id = 'CharlesRiverLaboratories_replicon_Repliconint_payrollexport'
sftp_conn_id = 'sftp_charlesriverlaboratories_gvecrep476'
pgp_conn_id = 'pgp_crl_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratories_603355'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

payroll_export_file_format = 'Canada ADP Export'
export = "Yes"

max_active_runs = 1
execution_timeout_days = 14


output_filepath = '/put'
secondary_encrypted_output_filepath = '/Production/Outbound/Canada ADP Payroll'
secondary_output_filepath = '/Production/Outbound/Canada ADP Payroll'

time_zone = "US/Eastern"

schedule_interval = "0 12,18 * * *"
can_run_batch_task_var_name = f'crl_canada_payroll_export_{instance}_can_run_batch_task'
CANADA_PAYROLL_CALENDER_MAPPER_TO_USE = CANADA_PAYROLL_CALENDER_MAPPER

adp_gv_system = 'P'
gv_system_number = '1'

employee_type = ('Hourly_Regular_Full-Time_Project','Hourly_Regular_Full-Time','Hourly_Regular_Part-Time','Hourly_Regular_Part-Time_Project',
              'Hourly_Temporary_Full-Time','Hourly_Temporary_Full-Time_Project','Hourly_Temporary_Part-Time','Hourly_Temporary_Part-Time_Project',
              'Temporary PT Hourly')

paycode = 'Temps non- payé/Temps non-payable'
