from crl.termination_balance_export_uk.config import *
from crl.termination_balance_export_uk.mapper.payroll_calendar_mapper import UK_PAYROLL_CALENDER_MAPPER
from crl.termination_balance_export_uk.mapper.time_off_balance_mapper import get_termination_timeoff_types


instance = 'prod'
environment = "production"

company_key = 'CharlesRiverLaboratories'
replicon_conn_id = 'CharlesRiverLaboratories_replicon_Repliconint_payrollexport'
sftp_conn_id = 'sftp_charlesriverlaboratories_gvecrep476'
pgp_conn_id = 'pgp_crl_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratories_603355'

tenant_email = 'WLM-PayrollTeam@crl.com,Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,RepliconSupport@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

secondary_sftp_conn_id = 'sftp_charlesriverlaboratories_603355'

date_time_format = "%m/%d/%Y, %H:%M:%S"

output_filepath = '/put/'
secondary_encrypted_output_filepath = '/Production/Outbound/UK ADP Payroll/Encrypted/'
log_filepath = '/put/'
secondary_output_filepath = '/Production/Outbound/UK ADP Payroll/Unencrypted/'

adp_gv_system = 'P'

UK_PAYROLL_CALENDER_MAPPER_TO_USE = UK_PAYROLL_CALENDER_MAPPER
termination_timeoff_types = get_termination_timeoff_types()

child_dag_id_udf_update = f'crl_termination_balance_uk_udf_update_child_{instance}'
master_dag_id = f'crl_termination_balance_uk_master_dag_{instance}'

