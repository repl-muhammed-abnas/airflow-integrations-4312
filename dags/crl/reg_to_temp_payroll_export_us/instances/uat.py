# pylint: disable=wildcard-import unused-wildcard-import
from crl.reg_to_temp_payroll_export_us.config import *
from crl.reg_to_temp_payroll_export_us.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER

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

can_run_batch_task_var_name = f"crl_regular_to_temp_batch_task_{instance}"

output_filepath = '/put/'
secondary_encrypted_output_filepath = '/Test/Outbound/USA ADP Payroll/'
log_filepath = '/put/'

udf_child_dag_id =f"crl_regular_to_temp_udf_update_usa_child_{instance}"
child_dag_id= f"crl_regular_to_temp_usa_child_{instance}"
master_dag_id = f"crl_regular_to_temp_dag_usa_master_{instance}"
master_dag_daily_id = f"crl_regular_to_temp_dag_usa_daily_master_{instance}"

USA_PAYROLL_CALENDAR = USA_PAYROLL_CALENDER_MAPPER

adp_gv_system = 'Q'
gv_system_number = '1'
file_name_prefix = 'PQ1476'

