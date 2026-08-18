# pylint: disable=wildcard-import unused-wildcard-import
from crl.payroll_balance_export_us_v1.config import *
from crl.payroll_balance_export_us_v1.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY,USA_PAYROLL_CALENDER_MAPPER_WEEKLY

instance = 'uat'


company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'CharlesRiverLaboratoriesSandbox_repliconint_payrollexport'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriestrial01_adp'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

secondary_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'
encrypt_output_file = True
file_name_prefix = 'PQ0476'


output_filepath = '/put/'
secondary_encrypted_output_filepath = '/Test/Outbound/USA ADP Payroll/'
log_filepath = '/put/'

USA_PAYROLL_CALENDER_MAPPER_TO_USE = USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY
USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY = USA_PAYROLL_CALENDER_MAPPER_WEEKLY

biweekly_dag_id = f"crl_payroll_balance_biweekly_usa_{instance}_v1"
weekly_dag_id = f"crl_payroll_balance_weekly_usa_{instance}_v1"

adp_gv_system = 'Q'
gv_system_number = '1'
