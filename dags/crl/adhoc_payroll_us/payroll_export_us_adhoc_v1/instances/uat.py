# pylint: disable=wildcard-import unused-wildcard-import
# pylint: disable=line-too-long
from crl.adhoc_payroll_us.payroll_export_us_adhoc_v1.config import *
from crl.adhoc_payroll_us.payroll_export_us_adhoc_v1.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY,USA_PAYROLL_CALENDER_MAPPER_WEEKLY
from crl.adhoc_payroll_us.payroll_export_us_adhoc_v1.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

instance = 'uat'


company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'CharlesRiverLaboratoriesSandbox_repliconint_payrollexport'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriestrial01_adp'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'




output_filepath = '/put'
secondary_encrypted_output_filepath = '/Test/Outbound/USA ADP Payroll'

biweekly_dag_id = f"crl_payroll_export_master_usa_adhoc_biweekly_v1_{instance}"
child_dag_id = f"create_object_usa_adhoc_child_v1_{instance}"
weekly_dag_id = f"crl_payroll_export_master_usa_adhoc_weekly_v1_{instance}"

REGULAR_EMPLOYEE_TYPES = REGULAR_TIME_EMPLOYEE_TYPES


schedule_interval = "0 7 * * *"

USA_PAYROLL_CALENDER_MAPPER_TO_USE = USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY
USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY = USA_PAYROLL_CALENDER_MAPPER_WEEKLY
crl_payroll_export_bearer_token_variable = "crl_payroll_export_bearer_token_variable_uat"
adp_gv_system = 'Q'
gv_system_number = '1'
