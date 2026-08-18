# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.morocco_overtime_payroll_export.config import *

instance = 'production'
location = 'Morocco'

environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_sopra_morocco_capgemini'

input_filepath = "/Outbound/MAR_Overtime_ZY90/Input"
s3_upload_filepath = "Capgemini/Outbound/MAR_Overtime_ZY90/Input"
ma01_filename_prefix = "ZY90_ABL_replicon_MA01"
ma02_ma03_filename_prefix = "ZY90_ERD_replicon_MA02_MA03"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_morocco_overtime_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_morocco_overtime_payroll_export_to_sopra_master_{instance}'
