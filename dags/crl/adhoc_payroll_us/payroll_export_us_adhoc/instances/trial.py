# pylint: disable=wildcard-import unused-wildcard-import
# pylint: disable=line-too-long
from crl.adhoc_payroll_us.payroll_export_us_adhoc.config import *
from crl.adhoc_payroll_us.payroll_export_us_adhoc.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY,USA_PAYROLL_CALENDER_MAPPER_WEEKLY
from crl.adhoc_payroll_us.payroll_export_us_adhoc.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

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

biweekly_dag_id = f"crl_payroll_export_master_usa_adhoc_biweekly_{instance}"
child_dag_id = f"create_object_usa_adhoc_child_{instance}"
weekly_dag_id = f"crl_payroll_export_master_usa_adhoc_weekly_{instance}"

REGULAR_EMPLOYEE_TYPES = REGULAR_TIME_EMPLOYEE_TYPES


schedule_interval = "0 7 * * *"

USA_PAYROLL_CALENDER_MAPPER_TO_USE = USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY
USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY = USA_PAYROLL_CALENDER_MAPPER_WEEKLY
crl_payroll_export_bearer_token_variable = "crl_payroll_export_bearer_token_variable_uat"
adp_gv_system = 'Q'
gv_system_number = '1'

disabled=True
