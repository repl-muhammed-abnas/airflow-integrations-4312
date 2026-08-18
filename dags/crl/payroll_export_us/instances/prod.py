# pylint: disable=wildcard-import unused-wildcard-import
# pylint: disable=line-too-long
from crl.payroll_export_us.config import *
from crl.payroll_export_us.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY, USA_PAYROLL_CALENDER_MAPPER_WEEKLY
from crl.payroll_export_us.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

instance = "prod"

environment = "production"

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

biweekly_dag_id = f"crl_payroll_export_usa_biweekly_master{instance}"
child_dag_id = f"crl_payroll_export_create_object_usa_child_{instance}"
weekly_dag_id = f"crl_payroll_export_usa_weekly_master{instance}"

REGULAR_EMPLOYEE_TYPES = REGULAR_TIME_EMPLOYEE_TYPES


USA_PAYROLL_CALENDER_MAPPER_TO_USE = USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY
USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY = USA_PAYROLL_CALENDER_MAPPER_WEEKLY

adp_gv_system = 'P'