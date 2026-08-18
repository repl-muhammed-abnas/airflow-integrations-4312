from crl.termination_balance_export_uk.config import *
from crl.termination_balance_export_uk.mapper.payroll_calendar_mapper import UK_PAYROLL_CALENDER_MAPPER
from crl.termination_balance_export_uk.mapper.time_off_balance_mapper import get_termination_timeoff_types


instance = 'uat'
environment = "pre-production"

company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'CharlesRiverLaboratoriesSandbox_repliconint_payrollexport'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriesSandbox_adp_gvectrep476'
pgp_conn_id = 'pgp_charlesriverlaboratories_sandbox_adp_payroll_export_uk'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

tenant_email = 'WLM-PayrollTeam@crl.com,Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

secondary_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

date_time_format = "%m/%d/%Y, %H:%M:%S"

output_filepath = '/put/'
secondary_encrypted_output_filepath = '/Test/Outbound/UK ADP Payroll/Encrypted/'
log_filepath = '/put/'
secondary_output_filepath = '/Test/Outbound/UK ADP Payroll/Unencrypted/'

adp_gv_system = 'Q'

UK_PAYROLL_CALENDER_MAPPER_TO_USE = UK_PAYROLL_CALENDER_MAPPER
termination_timeoff_types = get_termination_timeoff_types()

child_dag_id_udf_update = f'crl_termination_balance_uk_udf_update_child_{instance}'
master_dag_id = f'crl_termination_balance_uk_master_dag_{instance}'

