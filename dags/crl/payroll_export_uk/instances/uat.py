from crl.payroll_export_uk.config import *
from crl.payroll_export_uk.mapper.payroll_calendar_mapper import UK_PAYROLL_CALENDER_MAPPER
from crl.payroll_export_uk.mapper.paycode_mapper import UK_2010_PAYCODE_MAPPER, UK_2001_TIMEOFF_MAPPER

instance = "uat"
environment = "pre-production"

company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'CharlesRiverLaboratoriesSandbox_repliconint_payrollexport'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriesSandbox_adp_gvectrep476'
pgp_conn_id = 'pgp_charlesriverlaboratories_sandbox_adp_payroll_export_uk'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

tenant_email = 'WLM-PayrollTeam@crl.com,Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

output_filepath = '/put'
secondary_encrypted_output_filepath = '/Test/Outbound/UK ADP Payroll/Encrypted'
secondary_output_filepath = '/Test/Outbound/UK ADP Payroll/Unencrypted'

timeoff_output_filepath = '/put'
timeoff_secondary_output_filepath = '/Test/Outbound/UK ADP Payroll/Unencrypted'
timeoff_secondary_encrypted_output_filepath = '/Test/Outbound/UK ADP Payroll/Encrypted'

master_dag_id = f"crl_payroll_export_uk_monthly_master_{instance}"
child_dag_id = f"crl_payroll_export_uk_create_object_child_{instance}"
payroll_export_child_dag_id = f"crl_payroll_export_uk_create_payroll_export_child_{instance}"
timeoff_export_child_dag_id = f"crl_payroll_export_uk_create_timeoff_export_child_{instance}"

UK_PAYROLL_CALENDER_MAPPER_TO_USE = UK_PAYROLL_CALENDER_MAPPER
UK_2010_PAYCODE_MAPPER_TO_USE = UK_2010_PAYCODE_MAPPER
UK_2001_TIMEOFF_MAPPER_TO_USE = UK_2001_TIMEOFF_MAPPER


adp_gv_system = 'Q'
