# pylint: disable=wildcard-import unused-wildcard-import
from crl.termination_export_us.config import *
from crl.termination_export_us.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER
from crl.termination_export_us.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

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

output_filepath = '/put'
secondary_encrypted_output_filepath = '/Test/Outbound/USA ADP Payroll'

USA_PAYROLL_CALENDER_MAPPER_TO_USE = USA_PAYROLL_CALENDER_MAPPER
REGULAR_TIME_EMPLOYEE = REGULAR_TIME_EMPLOYEE_TYPES

adp_gv_system = 'Q'
gv_system_number = '1'

child_dag_id =f"create_object_termination_usa_child_{instance}"
master_dag_id = f"crl_termination_export_master_usa_{instance}"
master_dag_daily_id = f"crl_termination_export_master_daily_usa_{instance}"

disabled=True
