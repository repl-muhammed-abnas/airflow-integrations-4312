# pylint: disable=wildcard-import unused-wildcard-import
from crl.termination_balance_export_us_v1.config import *
from crl.termination_balance_export_us_v1.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER
from crl.termination_balance_export_us_v1.mapper.time_off_mapper import USA_TIMEOFF_TYPE_MAPPER

instance = 'uat'


company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'charlesriverlaboratoriessandbox_repliconint_payrollexport'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriestrial01_adp'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

secondary_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'
encrypt_output_file = True

output_filepath = '/put/'
secondary_encrypted_output_filepath = '/Test/Outbound/USA ADP Payroll/'
log_filepath = '/put/'

udf_child_dag_id =f"crl_termination_balance_udf_update_usa_child_v1_{instance}"
child_dag_id= f"crl_terminationbalance_usa_child_v1_{instance}"
master_dag_id = f"crl_termination_balance_dag_usa_master_v1_{instance}"
master_dag_daily_id = f"crl_termination_balance_dag_usa_daily_master_v1_{instance}"

remove_timeoff_templates_child_dag_id = f"crl_termination_balance_usa_remove_timeoff_template_child_v1_{instance}"

USA_PAYROLL_CALENDAR = USA_PAYROLL_CALENDER_MAPPER
USA_TIMEOFF_TYPE = USA_TIMEOFF_TYPE_MAPPER

adp_gv_system = 'Q'
gv_system_number = '1'

