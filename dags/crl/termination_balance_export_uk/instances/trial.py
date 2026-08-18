from crl.termination_balance_export_uk.config import *
from crl.termination_balance_export_uk.mapper.payroll_calendar_mapper import UK_PAYROLL_CALENDER_MAPPER
from crl.termination_balance_export_uk.mapper.time_off_balance_mapper import get_termination_timeoff_types


instance = 'trial'
environment = "pre-production"

company_key = 'CharlesRiverLaboratoriestrial01'
replicon_conn_id = 'charlesriverlaboratoriestrial01_repliconint_payrollexport'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = 'pgp_crl_uk_trial_adp_terminationbal_export'
secondary_encrypted_sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

secondary_sftp_conn_id = 'sftp_useast2'

date_time_format = "%m/%d/%Y, %H:%M:%S"

output_filepath = '/abnas/terminationexport/output/'
secondary_encrypted_output_filepath = '/abnas/terminationexport/encrypted/'
log_filepath = '/abnas/terminationexport/output/'
secondary_output_filepath = '/abnas/terminationexport/secondary/'

adp_gv_system = 'Q'

UK_PAYROLL_CALENDER_MAPPER_TO_USE = UK_PAYROLL_CALENDER_MAPPER
termination_timeoff_types = get_termination_timeoff_types()

child_dag_id_udf_update = f'crl_termination_balance_uk_udf_update_child_{instance}'
master_dag_id = f'crl_termination_balance_uk_master_dag_{instance}'


disabled=True
