# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.morocco_overtime_payroll_export.config import *

instance = 'uat2'
location = 'Morocco'

environment = 'pre-production'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_morocco_capgeminiuat2'

input_filepath = "/Outbound/MAR_Overtime_ZY90UAT2/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/MAR_Overtime_ZY90UAT2/Input"
ma01_filename_prefix = "ZY90_UAT2_replicon_MA01"
ma02_ma03_filename_prefix = "ZY90_UAT2_replicon_MA02_MA03"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_morocco_overtime_payroll_export_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_morocco_overtime_payroll_export_to_sopra_master_{instance}'
