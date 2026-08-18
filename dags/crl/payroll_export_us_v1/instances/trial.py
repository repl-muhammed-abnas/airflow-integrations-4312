# pylint: disable=wildcard-import unused-wildcard-import
# pylint: disable=line-too-long
from crl.payroll_export_us_v1.config import *
from crl.payroll_export_us_v1.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY, USA_PAYROLL_CALENDER_MAPPER_WEEKLY
from crl.payroll_export_us_v1.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

instance = 'trial'


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

biweekly_dag_id = f"crl_payroll_export_usa_biweekly_master_{instance}_v1"
child_dag_id = f"crl_payroll_export_create_object_usa_child_{instance}_v1"
weekly_dag_id = f"crl_payroll_export_usa_weekly_master_{instance}_v1"

REGULAR_EMPLOYEE_TYPES = REGULAR_TIME_EMPLOYEE_TYPES


USA_PAYROLL_CALENDER_MAPPER_TO_USE = USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY
USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY = USA_PAYROLL_CALENDER_MAPPER_WEEKLY
crl_payroll_export_bearer_token_variable = "crl_payroll_export_bearer_token_variable_uat"
adp_gv_system = 'Q'
gv_system_number = '1'

disabled=True
