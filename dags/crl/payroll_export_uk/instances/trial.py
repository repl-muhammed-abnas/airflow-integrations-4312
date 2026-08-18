from crl.payroll_export_uk.config import *
from crl.payroll_export_uk.mapper.payroll_calendar_mapper import UK_PAYROLL_CALENDER_MAPPER
from crl.payroll_export_uk.mapper.paycode_mapper import UK_2010_PAYCODE_MAPPER, UK_2001_TIMEOFF_MAPPER

instance = "trial"
environment = "pre-production"

company_key = 'CharlesRiverLaboratoriestrial01'
replicon_conn_id = 'charlesriverlaboratoriestrial01_repliconint_payrollexport'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = 'pgp_crl_uk_trial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

output_filepath = '/abnas/payroll_export/output'
secondary_encrypted_output_filepath = '/abnas/payroll_export/encrypted'
secondary_output_filepath = '/abnas/payroll_export/secondary'

timeoff_output_filepath = '/abnas/timeoff_details_export/output'
timeoff_secondary_output_filepath = '/abnas/timeoff_details_export/secondary'
timeoff_secondary_encrypted_output_filepath = '/abnas/timeoff_details_export/encrypted'

master_dag_id = f"crl_payroll_export_uk_monthly_master_{instance}"
child_dag_id = f"crl_payroll_export_uk_create_object_child_{instance}"
payroll_export_child_dag_id = f"crl_payroll_export_uk_create_payroll_export_child_{instance}"
timeoff_export_child_dag_id = f"crl_payroll_export_uk_create_timeoff_export_child_{instance}"

UK_PAYROLL_CALENDER_MAPPER_TO_USE = UK_PAYROLL_CALENDER_MAPPER
UK_2010_PAYCODE_MAPPER_TO_USE = UK_2010_PAYCODE_MAPPER
UK_2001_TIMEOFF_MAPPER_TO_USE = UK_2001_TIMEOFF_MAPPER


adp_gv_system = 'Q'

disabled=True
