# pylint: disable=wildcard-import unused-wildcard-import
from crl.termination_export.config import *
from crl.termination_export.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER
from crl.termination_export.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

instance = 'prod'

region = 'us-east-1'
environment = 'production'

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
REGULAR_TIME_EMPLOYEE_TYPES_TO_USE = REGULAR_TIME_EMPLOYEE_TYPES

run_dag_payroll = True

adp_gv_system = 'P'
gv_system_number = '1'
